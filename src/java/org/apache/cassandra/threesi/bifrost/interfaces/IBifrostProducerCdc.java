package org.apache.cassandra.threesi.bifrost.interfaces;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;

import java.io.IOException;
import java.sql.SQLException;

public interface IBifrostProducerCdc {
    void start() throws SQLException;

    void process(PartitionUpdate partitionUpdate, Mutation mutation) throws SQLException, IOException;
}
