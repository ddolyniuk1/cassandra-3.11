package org.apache.cassandra.threesi.bifrost.services;

public interface BifrostImporterServiceMBean {

    @SuppressWarnings("UnusedReturnValue")
    int importMutations(String filePath, int limit, String keyspaceFilter, String tableFilter);
}
