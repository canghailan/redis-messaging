package cc.whohow.messaging.redis.pulsar;

import cc.whohow.messaging.redis.QueryParameters;
import cc.whohow.messaging.redis.Redis;
import cc.whohow.messaging.redis.RedisMessaging;
import cc.whohow.messaging.redis.SpringConfigurator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/pulsar/ws/v2/producer/persistent/{tenant}/{namespace}/{topic}", configurator = SpringConfigurator.class)
public class PulsarProducer {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarProducer.class);
    private final LongAdder messageCounter = new LongAdder();

    @Inject
    private ObjectMapper objectMapper;
    @Inject
    private Redis redis;

    private Session session;
    private String tenant;
    private String namespace;
    private String topic;
    private long sendTimeoutMillis;
    private boolean batchingEnabled;
    private int batchingMaxMessages;
    private int maxPendingMessages;
    private long batchingMaxPublishDelay;
    private String messageRoutingMode;
    private String compressionType;
    private String producerName;
    private long initialSequenceId;
    private String hashingScheme;
    private String token;
    private String redisKey;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("tenant") String tenant,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) {
        LOG.debug("{} Open: producer/{}/{}/{}", session.getId(), tenant, namespace, topic);

        QueryParameters queryParameters = new QueryParameters(session.getRequestParameterMap());

        this.session = session;
        this.tenant = tenant;
        this.namespace = namespace;
        this.topic = topic;
        this.sendTimeoutMillis = queryParameters.get("sendTimeoutMillis")
                .map(Long::parseLong)
                .orElse(30L * 1000L);
        this.batchingEnabled = queryParameters.get("batchingEnabled")
                .map(Boolean::parseBoolean)
                .orElse(false);
        this.batchingMaxMessages = queryParameters.get("batchingMaxMessages")
                .map(Integer::parseInt)
                .orElse(1000);
        this.maxPendingMessages = queryParameters.get("maxPendingMessages")
                .map(Integer::parseInt)
                .orElse(1000);
        this.batchingMaxPublishDelay = queryParameters.get("batchingMaxPublishDelay")
                .map(Long::parseLong)
                .orElse(30L * 1000L);
        this.messageRoutingMode = queryParameters.get("messageRoutingMode").orElse(null);
        this.compressionType = queryParameters.get("compressionType").orElse(null);
        this.producerName = queryParameters.get("producerName").orElse(null);
        this.initialSequenceId = queryParameters.get("initialSequenceId")
                .map(Long::parseLong)
                .orElse(30L * 1000L);
        this.hashingScheme = queryParameters.get("hashingScheme").orElse(null);
        this.token = queryParameters.get("token").orElse(null);
        this.redisKey = RedisMessaging.toRedisKey(tenant, namespace, topic);
    }

    @OnMessage
    public void onMessage(String text) throws Exception {
        LOG.trace("{} Message: producer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, text);

        JsonNode message = objectMapper.readTree(text);
        JsonNode payload = message.get(RedisMessaging.PAYLOAD);
        JsonNode properties = message.path(RedisMessaging.PROPERTIES);
        JsonNode context = message.path(RedisMessaging.CONTEXT);
        JsonNode key = message.path(RedisMessaging.KEY);
        JsonNode replicationClusters = message.path(RedisMessaging.REPLICATION_CLUSTERS);

        CommandArgs<byte[], byte[]> commandArgs = new CommandArgs<>(Redis.CODEC)
                .add(redisKey).add("*");
        if (!properties.isEmpty()) {
            Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                commandArgs.add(RedisMessaging.PROPERTIES_PREFIX + field.getKey()).add(field.getValue().textValue());
            }
        }
        commandArgs.add(RedisMessaging.PAYLOAD).add(payload.binaryValue());
        if (!context.isMissingNode()) {
            commandArgs.add(RedisMessaging.CONTEXT).add(context.asText());
        }
        if (!key.isMissingNode()) {
            commandArgs.add(RedisMessaging.KEY).add(key.asText());
        }
        if (!replicationClusters.isMissingNode()) {
            commandArgs.add(RedisMessaging.REPLICATION_CLUSTERS).add(replicationClusters.toString());
        }
        if (producerName != null) {
            commandArgs.add(RedisMessaging.PRODUCER_NAME).add(producerName);
        }

        CommandOutput<byte[], byte[], List<String>> commandOutput = new StringListOutput<>(Redis.CODEC);
        commandOutput.multi(1);

        LOG.trace("XADD {} * {}", this.redisKey, text);
        redis.executeAsync(CommandType.XADD, commandArgs, commandOutput)
                .whenComplete((r, e) -> {
                    ObjectNode response = objectMapper.createObjectNode();
                    if (e != null) {
                        LOG.error(e.getMessage(), e);
                        response.put("result", "send-error:8")
                                .put("errorMsg", e.getMessage())
                                .set(RedisMessaging.CONTEXT, context);
                    } else {
                        response.put("result", "ok")
                                .put("messageId", r.get(0))
                                .set(RedisMessaging.CONTEXT, context);
                    }
                    session.getAsyncRemote().sendText(response.toString());
                });
    }


    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: producer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, closeReason.getReasonPhrase());
    }
}
