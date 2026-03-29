package org.apache.cassandra.threesi.bifrost.interfaces;

public interface IBifrostConsumer {
    int importMutations(String sourceSqlitePath, int limit, String keyspaceFilter, String tableFilter);

    boolean canImportFile(String filePath);
}
