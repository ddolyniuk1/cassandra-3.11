package org.apache.cassandra.threesi.bifrost.sqlite;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.threesi.bifrost.interfaces.IBifrostConsumer;
import org.apache.cassandra.threesi.bifrost.services.BifrostImporterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.*;
import java.util.Base64;
import java.util.Collections;
import java.util.UUID;

public class BifrostSqliteConsumer implements IBifrostConsumer {
    private static final Logger logger = LoggerFactory.getLogger(BifrostSqliteConsumer.class);

    @Override
    public int importMutations(String sourceSqlitePath, int limit, String keyspaceFilter, String tableFilter) {
        File dbFile = new File(sourceSqlitePath);
        if (!dbFile.exists()) {
            logger.error("SQLite database file does not exist: {}", sourceSqlitePath);
            return 0;
        }

        String importId = UUID.randomUUID().toString();
        int processedCount = 0;
        int appliedCount = 0;
        int errorCount = 0;

        logger.info("Starting mutation import from: {} (importId: {})", sourceSqlitePath, importId);

        String query = buildSelectQuery(keyspaceFilter, tableFilter, limit);

        try (Connection sourceConn = openConnection(sourceSqlitePath)) {
            try (PreparedStatement selectStmt = sourceConn.prepareStatement(query);
                 PreparedStatement deleteStmt = sourceConn.prepareStatement(
                         "DELETE FROM cdc_events WHERE id = ?"))
            {
                sourceConn.setAutoCommit(false);
                bindSelectParameters(selectStmt, keyspaceFilter, tableFilter, limit);
                ResultSet rs = selectStmt.executeQuery();

                while (rs.next()) {
                    long eventId = rs.getLong("id");
                    String keyspace = rs.getString("keyspace");
                    String tableName = rs.getString("table_name");
                    String operation = rs.getString("operation");
                    String mutationData = rs.getString("data");
                    String mutationHash = rs.getString("mutation_hash");
                    processedCount++;

                    try {
                        Mutation mutation = deserializeMutation(mutationData);
                        if (mutation == null) {
                            logger.warn("Could not deserialize mutation {}: {}", eventId, mutationHash);
                            errorCount++;
                            continue;
                        }

                        if (!mutation.getKeyspaceName().equals(keyspace)) {
                            logger.warn("Keyspace mismatch for mutation {}: expected {}, got {}",
                                    eventId, keyspace, mutation.getKeyspaceName());
                            errorCount++;
                            continue;
                        }

                        BifrostImporterService.getInstance()
                                .setIsRecentlyProcessedMutation(mutationHash);
                        applyMutation(mutation);

                        deleteStmt.setLong(1, eventId);
                        deleteStmt.addBatch();
                        appliedCount++;

                        if (logger.isDebugEnabled()) {
                            logger.debug("Applied mutation {}: {}.{} ({})",
                                    eventId, keyspace, tableName, operation);
                        }
                    } catch (Exception e) {
                        errorCount++;
                        logger.error("Error applying mutation {}: {}.{}",
                                eventId, keyspace, tableName, e);
                    }

                    if (processedCount % 1000 == 0) {
                        deleteStmt.executeBatch();
                        sourceConn.commit();
                        logger.info("Import progress: {} processed, {} applied, {} errors",
                                processedCount, appliedCount, errorCount);
                    }
                }

                // flush any remaining deletes
                deleteStmt.executeBatch();
                sourceConn.commit();
            }

            // reclaim space if enough free pages have accumulated
            try (Statement pragmaStmt = sourceConn.createStatement()) {
                ResultSet pageInfo = pragmaStmt.executeQuery(
                        "SELECT freelist_count, page_count FROM pragma_freelist_count(), pragma_page_count()");
                if (pageInfo.next()) {
                    long freePages = pageInfo.getLong(1);
                    long totalPages = pageInfo.getLong(2);
                    if (totalPages > 0 && (double) freePages / totalPages > 0.5) {
                        logger.info("Vacuuming SQLite database ({} free / {} total pages)", freePages, totalPages);
                        try (Statement vacuumStmt = sourceConn.createStatement()) {
                            vacuumStmt.execute("VACUUM");
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Vacuum check failed, will retry next cycle", e);
            }

        } catch (Exception e) {
            logger.error("Error during mutation import from: " + sourceSqlitePath, e);
        }

        logger.info("Import completed from {}: {} processed, {} applied, {} errors (importId: {})",
                sourceSqlitePath, processedCount, appliedCount, errorCount, importId);

        return appliedCount;
    }
    @Override
    public boolean canImportFile(String filePath) {
        return filePath != null && filePath.endsWith(".db");
    }

    private void applyMutation(Mutation mutation) throws WriteTimeoutException, WriteFailureException, OverloadedException {
        StorageProxy.mutate(
                Collections.singletonList(mutation),
                ConsistencyLevel.LOCAL_ONE,
                System.nanoTime()
        );
    }

    private Mutation deserializeMutation(String mutationData) {
        try {
            byte[] data = Base64.getDecoder().decode(mutationData);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputPlus.DataInputStreamPlus dis = new DataInputPlus.DataInputStreamPlus(bais);
            return Mutation.serializer.deserialize(
                    dis, org.apache.cassandra.net.MessagingService.current_version);
        } catch (Exception e) {
            logger.warn("Failed to deserialize mutation data", e);
            return null;
        }
    }

    private Connection openConnection(String dbPath) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA synchronous=NORMAL;");
            stmt.execute("PRAGMA busy_timeout=30000;");
        }
        return conn;
    }

    private String buildSelectQuery(String keyspaceFilter, String tableFilter, int limit) {
        StringBuilder qb = new StringBuilder(
                "SELECT id, keyspace, table_name, operation, data, mutation_hash " +
                        "FROM cdc_events WHERE processed != 1"
        );

        if (keyspaceFilter != null && !keyspaceFilter.trim().isEmpty())
            qb.append(" AND keyspace = ?");

        if (tableFilter != null && !tableFilter.trim().isEmpty())
            qb.append(" AND table_name = ?");

        qb.append(" ORDER BY cassandra_write_time ASC");

        if (limit > 0)
            qb.append(" LIMIT ?");

        return qb.toString();
    }

    private void bindSelectParameters(PreparedStatement stmt, String keyspaceFilter,
                                      String tableFilter, int limit) throws SQLException {
        int idx = 1;
        if (keyspaceFilter != null && !keyspaceFilter.trim().isEmpty())
            stmt.setString(idx++, keyspaceFilter);
        if (tableFilter != null && !tableFilter.trim().isEmpty())
            stmt.setString(idx++, tableFilter);
        if (limit > 0)
            stmt.setInt(idx, limit);
    }
}