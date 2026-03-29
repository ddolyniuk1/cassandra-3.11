package org.apache.cassandra.threesi.bifrost.sqlite;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.threesi.bifrost.interfaces.IBifrostProducer;
import org.apache.cassandra.threesi.bifrost.services.BifrostImporterService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.*;
import java.util.Base64;
import java.util.List;
import java.util.UUID;


public class BifrostSqliteProducer implements IBifrostProducer {
    private static final Logger logger = LoggerFactory.getLogger(BifrostSqliteProducerCdc.class);
    private String sqlitePath;
    private String hostId = null;
    /**
     * Initialize our producer
     * @throws SQLException
     */
    @Override
    public void start() throws SQLException {
        if(!DatabaseDescriptor.isBifrostEnabled()) {
            return;
        }

        String cdcRawDirectory = DatabaseDescriptor.getCDCLogLocation();
        if (cdcRawDirectory == null || cdcRawDirectory.trim().isEmpty())
        {
            logger.error("Data Bridge enabled but cdc_raw_directory not configured");
            return;
        }

        sqlitePath = Paths.get(cdcRawDirectory, "cdc-mutations.db").toString();

        initializeDatabase(sqlitePath);
    }
    @Override
    public void process(List<Mutation> mutations) throws SQLException, IOException
    {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath))
        {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT OR IGNORE INTO cdc_events " +
                            "(keyspace, table_name, operation, data, mutation_hash, " +
                            "cassandra_write_time, source_node_id) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)"))
            {
                if (hostId == null)
                    hostId = resolveSourceNodeId();

                for (Mutation mutation : mutations)
                {
                    for (PartitionUpdate pu : mutation.getPartitionUpdates())
                    {
                        byte[] serialized = serializeMutation(mutation);
                        String hash = calculateMutationHash(serialized, mutation, pu);

                        if (Boolean.TRUE.equals(
                                BifrostImporterService.getInstance()
                                        .getIsRecentlyProcessedMutation(hash)))
                            continue;

                        stmt.setString(1, mutation.getKeyspaceName());
                        stmt.setString(2, pu.metadata().cfName);
                        stmt.setString(3, determineOperation(pu));
                        stmt.setString(4, formatMutationData(serialized));
                        stmt.setString(5, hash);
                        stmt.setLong(6, pu.maxTimestamp());
                        stmt.setString(7, hostId);
                        stmt.addBatch();
                    }
                }

                stmt.executeBatch();
                conn.commit();
            }
        }
    }

    private static byte[] serializeMutation(Mutation mutation) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             WrappedDataOutputStreamPlus dos = new WrappedDataOutputStreamPlus(byteArrayOutputStream)) {

            Mutation.serializer.serialize(mutation, dos, org.apache.cassandra.net.MessagingService.current_version);
            return byteArrayOutputStream.toByteArray();
        }
    }

    private String resolveSourceNodeId() {
        try {
            String nodeId = DatabaseDescriptor.getBifrostNodeId();
            if(StringUtils.isNotBlank(nodeId)) {
                return nodeId;
            }
        }
        catch(Exception e) {
            logger.warn("StorageService not ready, failed to obtain Bifrost node ID.");
        }
        try {
            UUID hostId = StorageService.instance.getLocalHostUUID();
            if (hostId != null) return hostId.toString();
        } catch (Exception e) {
            logger.warn("StorageService not ready, falling back to broadcast address");
        }
        return DatabaseDescriptor.getBroadcastAddress().getHostAddress();
    }
    
    /**
     * Generate a hash of the content of the mutation
     * Should allow for data based deduplication using the timestamp of the mutation as well as content
     * @param mutation
     * @param partitionUpdate
     * @return
     */
    private String calculateMutationHash(byte[] serializedData,
                                         Mutation mutation,
                                         PartitionUpdate partitionUpdate) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(mutation.getKeyspaceName().getBytes());
            md.update(partitionUpdate.metadata().cfName.getBytes());
            md.update(serializedData); // the actual content
            byte[] hashBytes = md.digest();
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (Exception e) {
            logger.warn("Failed to calculate mutation hash", e);
            return UUID.randomUUID().toString();
        }
    }
    /**
     * Serializes a mutation and return a base64 string of the serialized bytes 
     * @param serializedMutationData
     * @return
     */
    private String formatMutationData(byte[] serializedMutationData)
    {
        if(serializedMutationData == null) return "";
        return Base64.getEncoder().encodeToString(serializedMutationData);
    }
    
    private String determineOperation(PartitionUpdate partitionUpdate)
    {
        if (partitionUpdate.isEmpty())
            return "DELETE";
        return "INSERT_UPDATE";
    }

    /**
     * Create our SQLite database
     * @param sqlitePath
     * @throws SQLException
     */
    private void initializeDatabase(String sqlitePath) throws SQLException
    {
        File dbFile = new File(sqlitePath);
        //noinspection ResultOfMethodCallIgnored
        dbFile.getParentFile().mkdirs();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
             Statement stmt = conn.createStatement())  {
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA synchronous=NORMAL;");
            stmt.execute("PRAGMA cache_size=1000;");
            stmt.execute("PRAGMA temp_store=memory;");
            stmt.execute("PRAGMA busy_timeout=30000;");

            // mutation hash must be unique - this will prevent duplicate insertions
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS cdc_events (" +
                            "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "keyspace TEXT NOT NULL, " +
                            "table_name TEXT NOT NULL, " +
                            "operation TEXT NOT NULL, " +
                            "data TEXT, " +
                            "mutation_hash TEXT NOT NULL UNIQUE, " +
                            "cassandra_write_time INTEGER NOT NULL, " +
                            "source_node_id TEXT NOT NULL, " +
                            "timestamp INTEGER DEFAULT (strftime('%s','now')), " +
                            "processed BOOLEAN DEFAULT FALSE)"
            );

            stmt.execute("CREATE INDEX IF NOT EXISTS idx_keyspace_table ON cdc_events (keyspace, table_name)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON cdc_events (timestamp)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_processed ON cdc_events (processed)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_mutation_hash ON cdc_events (mutation_hash)");

            logger.info("SQLite database initialized: {}", sqlitePath);
        }
    }
}
