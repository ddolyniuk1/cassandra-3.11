package org.apache.cassandra.threesi.bifrost.interfaces;

import org.apache.cassandra.db.Mutation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public interface IBifrostProducer {
    void start() throws SQLException;

    void process(List<Mutation> mutations) throws SQLException, IOException;
}
