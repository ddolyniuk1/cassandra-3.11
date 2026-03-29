package org.apache.cassandra.threesi.bifrost.facades;

import org.apache.cassandra.threesi.bifrost.interfaces.IBifrostConsumer;
import org.apache.cassandra.threesi.bifrost.sqlite.BifrostSqliteConsumer;

import java.util.concurrent.ConcurrentHashMap;

public class BifrostConsumerFacade implements IBifrostConsumer {
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") // we will add to this later
    private final ConcurrentHashMap<String, IBifrostConsumer> importers = new ConcurrentHashMap<>();
    
    public BifrostConsumerFacade() {
        importers.put(BifrostSqliteConsumer.class.getName(), new BifrostSqliteConsumer());
    }

    @Override
    public int importMutations(String filePath, int limit, String keyspaceFilter, String tableFilter) {
        return importers
                .values()
                .stream()
                .filter((importer) -> importer.canImportFile(filePath))
                .findFirst()
                .map((importer) -> importer.importMutations(filePath, limit, keyspaceFilter, tableFilter))
                .orElse(0);
    }

    @Override
    public boolean canImportFile(String filePath) {
        return false;
    }
}
