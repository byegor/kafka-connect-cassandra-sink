package com.begor.kc.cassandra;

import com.begor.kc.cassandra.processors.MessageProcessor;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class CassandraSinkConnectorConfig extends AbstractConfig {
    public final int port;
    public final String[] contactPoints;
    public final DefaultConsistencyLevel consistencyLevel;
    public final String localDataCenter;
    public final long statementTimeoutMs;
    public final Map<String, List<MessageProcessor>> messageProcessorsByTopic;
    public static final String PORT_CONFIG = "cassandra.port";
    public static final int PORT_CONFIG_DEFAULT = 9042;
    static final String PORT_DOC = "The port the Cassandra hosts are listening on.";

    public static final String CONTACT_POINTS_CONFIG = "cassandra.contact.points";
    static final String CONTACT_POINTS_DOC = "The Cassandra hosts to connect to.";

    public static final String CONSISTENCY_LEVEL_CONFIG = "cassandra.consistency.level";
    public static final String CONSISTENCY_LEVEL_DEFAULT = DefaultConsistencyLevel.LOCAL_QUORUM.name();
    static final String CONSISTENCY_LEVEL_DOC = "The requested consistency level to use when writing to Cassandra.";

    public static final String EXECUTE_STATEMENT_TIMEOUT_MS_CONF = "cassandra.execute.timeout.ms";

    public static final String LOCAL_DATACENTER_CONFIG = "cassandra.local.datacenter";
    static final String LOCAL_DATACENTER_DOC = "The local datacenter of the cassandra nodes defined in cassandra.contact.points. To get the local datacenter  of a node using the cqlsh tool, connect to your node and run the following cqlsh command: `select data_center from system.local;` This configuration is required for the connector to start.";

    public static final String MESSAGE_PROCESSOR = "task.message.processors";
    static final String MESSAGE_PROCESSOR_DOC = "Message processor that will convert kafka message to cassandra statement. Should be presented as json, i.e. map[ topicName -> [message processors]";

    public CassandraSinkConnectorConfig(final Map<?, ?> originals) {
        super(config(), originals);
        this.port = this.getInt(PORT_CONFIG);
        final List<String> contactPoints = this.getList(CONTACT_POINTS_CONFIG);
        this.contactPoints = contactPoints.toArray(new String[0]);
        String consistency = this.getString(CONSISTENCY_LEVEL_CONFIG);
        this.consistencyLevel = DefaultConsistencyLevel.valueOf(consistency);
        this.localDataCenter = this.getString(LOCAL_DATACENTER_CONFIG);
        this.statementTimeoutMs = this.getLong(EXECUTE_STATEMENT_TIMEOUT_MS_CONF);
        this.messageProcessorsByTopic = initMessageProcessors();
    }

    private Map<String, List<MessageProcessor>> initMessageProcessors() {
        Map<String, List<MessageProcessor>> result = new HashMap<>();
        String jsonValue = getString(MESSAGE_PROCESSOR);
        try {
            JSONObject json = new JSONObject(jsonValue);
            Iterator keys = json.keys();
            while (keys.hasNext()) {
                String topicName = (String) keys.next();
                JSONArray jsonArray = json.getJSONArray(topicName);
                List<MessageProcessor> processors = new ArrayList<>(jsonArray.length());
                for (int i = 0; i < jsonArray.length(); i++) {
                    String className = jsonArray.getString(i);
                    Object messageProcessor = Class.forName(className).getConstructor().newInstance();
                    processors.add((MessageProcessor) messageProcessor);
                }
                result.put(topicName, processors);
            }
            return result;
        } catch (JSONException e) {
            throw new ConnectException("Failed to parse " + MESSAGE_PROCESSOR + " property", e);
        } catch (Exception e) {
            throw new ConnectException("Failed to instantiate message parsers: ", e);
        }
    }

    public static ConfigDef config() {
        ConfigDef configDef = new ConfigDef();

        final String group = "connection";
        int orderInGroup = 0;


        configDef.define(
                        "name",
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Connector name",
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        "Connector name"
                )
                .define(
                        CONTACT_POINTS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        CONTACT_POINTS_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        "Contact points"
                )
                .define(
                        PORT_CONFIG,
                        ConfigDef.Type.INT,
                        PORT_CONFIG_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        PORT_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        "Contact points port"
                )
                .define(
                        LOCAL_DATACENTER_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        LOCAL_DATACENTER_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        "Local data center name"
                )
                .define(
                        CONSISTENCY_LEVEL_CONFIG,
                        ConfigDef.Type.STRING,
                        CONSISTENCY_LEVEL_DEFAULT,
                        ConfigDef.ValidString.in(Arrays.stream(DefaultConsistencyLevel.values()).map(Enum::name).toArray(String[]::new)),
                        ConfigDef.Importance.LOW,
                        CONSISTENCY_LEVEL_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        "Consistency level"
                )
                .define(
                        EXECUTE_STATEMENT_TIMEOUT_MS_CONF,
                        ConfigDef.Type.INT,
                        30000,
                        ConfigDef.Importance.LOW,
                        "Statement timeout",
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        "Statement timeout"
                )
                .define(
                        MESSAGE_PROCESSOR,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.MEDIUM,
                        MESSAGE_PROCESSOR_DOC,
                        "Task Execution",
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        "Message processor"
                );
        return configDef;
    }
}
