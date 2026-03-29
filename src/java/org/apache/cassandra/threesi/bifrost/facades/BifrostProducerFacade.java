package org.apache.cassandra.threesi.bifrost.facades;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.threesi.bifrost.interfaces.IBifrostProducer;
import org.apache.cassandra.threesi.bifrost.services.MutationListener;
import org.apache.cassandra.threesi.bifrost.sqlite.BifrostSqliteProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BifrostProducerFacade implements IBifrostProducer {
    private static final Logger logger = LoggerFactory.getLogger(BifrostProducerFacade.class);
    private static BifrostProducerFacade _instance;
    private final ConcurrentHashMap<String, IBifrostProducer> processors = new ConcurrentHashMap<>();
    
    public BifrostProducerFacade() {
        Register(new BifrostSqliteProducer());
    }

    public static IBifrostProducer getInstance() {
        if(_instance == null) {
            _instance = new BifrostProducerFacade();
        }
        return _instance;
    }

    private void Register(IBifrostProducer processor) {
        processors.put(processor.getClass().getName(), processor);
    }

    @Override
    public void start() throws SQLException {
        MutationListener listener = new MutationListener(this);
        MutationListener.setInstance(listener);
        listener.enable();

        logger.info("Bifrost listening on write path directly");
        for(IBifrostProducer processor : processors.values()) {
            processor.start();
        }
    }

    @Override
    public void process(List<Mutation> mutations) throws SQLException, IOException {
        for(IBifrostProducer processor : processors.values()) {
            try {
                processor.process(mutations);
            } catch (Exception exception) {
                logger.error("An exception occurred processing a partition update for a mutation!", exception);
            }
        }
    }

//    @Override legacy CDC based logic below
//    public void process(PartitionUpdate partitionUpdate, Mutation mutation) {
//        for(IBifrostProducer processor : processors.values()) {
//            try {
//                processor.process(partitionUpdate, mutation);
//            } catch (Exception exception) {
//                logger.error("An exception occurred processing a partition update for a mutation!", exception);
//            }
//        }
//    }
}
