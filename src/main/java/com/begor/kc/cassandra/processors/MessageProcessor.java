package com.begor.kc.cassandra.processors;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.function.Consumer;
import java.util.function.Predicate;

public interface MessageProcessor extends Predicate<SinkRecord> {

    BoundStatement process(SinkRecord record);
}
