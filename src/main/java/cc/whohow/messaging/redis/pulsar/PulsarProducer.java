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
import java.util.List;
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
    private String key;
    private Session session;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("tenant") String tenant,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) {
        LOG.debug("{} Open: producer/{}/{}/{}", session.getId(), tenant, namespace, topic);

        QueryParameters queryParameters = new QueryParameters(session.getRequestParameterMap());

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
        this.key = RedisMessaging.key(tenant, namespace, topic);

        this.session = session;
    }

    @OnMessage
    public void onMessage(String text) throws Exception {
        LOG.trace("{} Message: producer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, text);

        JsonNode message = objectMapper.readTree(text);
        JsonNode payload = message.get("payload");
        JsonNode properties = message.path("properties");
        JsonNode context = message.path("context");
        JsonNode key = message.path("key");
        JsonNode replicationClusters = message.path("replicationClusters");

        CommandArgs<byte[], byte[]> commandArgs = new CommandArgs<>(Redis.CODEC)
                .add(this.key).add("*");
        if (producerName != null) {
            commandArgs.add("producerName").add(producerName);
        }
        commandArgs.add("payload").add(payload.binaryValue());
        if (!properties.isMissingNode()) {
            commandArgs.add("properties").add(properties.toString());
        }
        if (!context.isMissingNode()) {
            commandArgs.add("context").add(context.asText());
        }
        if (!key.isMissingNode()) {
            commandArgs.add("key").add(key.asText());
        }
        if (!replicationClusters.isMissingNode()) {
            commandArgs.add("replicationClusters").add(replicationClusters.toString());
        }

        CommandOutput<byte[], byte[], List<String>> commandOutput = new StringListOutput<>(Redis.CODEC);
        commandOutput.multi(1);

        LOG.trace("XADD {} * {}", this.key, text);
        redis.executeAsync(CommandType.XADD, commandArgs, commandOutput)
                .whenComplete((r, e) -> {
                    ObjectNode response = objectMapper.createObjectNode();
                    if (e != null) {
                        LOG.error(e.getMessage(), e);
                        response.put("result", "send-error:8")
                                .put("errorMsg", e.getMessage())
                                .set("context", context);
                    } else {
                        response.put("result", "ok")
                                .put("messageId", r.get(0))
                                .set("context", context);
                    }
                    session.getAsyncRemote().sendText(response.toString());
                });
    }


    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: producer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, closeReason.getReasonPhrase());
    }
}
