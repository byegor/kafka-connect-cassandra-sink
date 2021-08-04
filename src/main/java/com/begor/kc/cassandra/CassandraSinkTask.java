// 
// Decompiled by Procyon v0.5.36
// 

package com.begor.kc.cassandra;

import com.begor.kc.cassandra.processors.MessageProcessor;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class CassandraSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
    private CassandraSinkConnectorConfig config;
    CqlSession session;
    boolean sessionConnectionIsValid = false;


    public String version() {
        return "1";
    }

    public void start(final Map<String, String> taskConfig) {
        this.config = new CassandraSinkConnectorConfig(taskConfig);
    }

    private CqlSession session() {
        if (session == null) {
            log.info("Creating Cassandra Session.");
            this.session = SessionFactory.createSession(config);
            sessionConnectionIsValid = true;
        } else if (!sessionConnectionIsValid) {
            log.warn("Cassandra Session is invalid. Closing and creating new.");
            close();
            this.session = SessionFactory.createSession(config);
            sessionConnectionIsValid = true;
        }
        return this.session;

    }

    public void put(final Collection<SinkRecord> records) {
        final List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();
        for (final SinkRecord record : records) {
            if (record.value() != null) {
                String topic = record.topic();
                List<MessageProcessor> processors = config.messageProcessorsByTopic.get(topic);
                for (MessageProcessor processor : processors) {
                    BoundStatement boundStatement = processor.process(record);
                    if(boundStatement != null){
                        boundStatement.setConsistencyLevel(config.consistencyLevel);
                        log.trace("put() - Executing Bound Statement for {}:{}:{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                        final CompletionStage<AsyncResultSet> resultSet = session().executeAsync(boundStatement);
                        futures.add(resultSet.toCompletableFuture());
                    }
                }
            }
        }

        if (futures.size() > 0) {
            try {
                log.debug("put() - Checking future(s)");
                for (final CompletableFuture<AsyncResultSet> future : futures) {
                    final AsyncResultSet set = future.get(this.config.statementTimeoutMs, TimeUnit.MILLISECONDS);
                }
                this.context.requestCommit();
            } catch (CancellationException | InterruptedException | ExecutionException ex) {
                log.debug("put() - Setting clusterValid = false", ex);
                sessionConnectionIsValid = false;
                throw new RetriableException(ex);
            } catch (TimeoutException ex) {
                log.error("put() - TimeoutException.", ex);
                throw new RetriableException(ex);
            } catch (Exception ex) {
                log.error("put() - Unknown exception. Setting clusterValid = false", ex);
                sessionConnectionIsValid = false;
                throw new RetriableException(ex);
            }
        }
    }

    void close() {
        if (null != session) {
            log.info("Closing session");
            try {
                session.close();
            } catch (Exception ex) {
                log.error("Exception thrown while closing session.", ex);
            }
            this.session = null;
        }
    }

    public void stop() {
        this.close();
    }

}
