package cc.whohow.messaging.redis.pulsar;

import cc.whohow.messaging.redis.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.ReplayOutput;
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
import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/pulsar/ws/v2/consumer/persistent/{tenant}/{namespace}/{topic}/{subscription}", configurator = SpringConfigurator.class)
public class PulsarConsumer implements RedisKeyspaceListener {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumer.class);
    private final LongAdder messageCounter = new LongAdder();
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final AtomicBoolean consumeMore = new AtomicBoolean(true);
    private final AtomicInteger permitMessages = new AtomicInteger(0);

    @Inject
    private Clock clock;
    @Inject
    private ObjectMapper objectMapper;
    @Inject
    private Redis redis;
    @Inject
    private RedisKeyspaceNotification redisKeyspaceNotification;

    private Session session;
    private String tenant;
    private String namespace;
    private String topic;
    private String subscription;
    private long ackTimeoutMillis;
    private String subscriptionType;
    private int receiverQueueSize;
    private String consumerName;
    private int priorityLevel;
    private int maxRedeliverCount;
    private String deadLetterTopic;
    private boolean pullMode;
    private int negativeAckRedeliveryDelay;
    private String token;
    private String redisKey;
    private String lastMessageId;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("tenant") String tenant,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic,
                       @PathParam("subscription") String subscription) {
        LOG.debug("{} Open: consumer/{}/{}/{}", session.getId(), tenant, namespace, topic);

        QueryParameters queryParameters = new QueryParameters(session.getRequestParameterMap());

        this.session = session;
        this.tenant = tenant;
        this.namespace = namespace;
        this.topic = topic;
        this.subscription = subscription;
        this.ackTimeoutMillis = queryParameters.get("ackTimeoutMillis")
                .map(Long::parseLong)
                .orElse(0L);
        this.subscriptionType = queryParameters.get("subscriptionType")
                .orElse("Exclusive");
        this.receiverQueueSize = queryParameters.get("receiverQueueSize")
                .map(Integer::parseInt)
                .orElse(1000);
        this.consumerName = queryParameters.get("consumerName")
                .orElse(session.getId());
        this.priorityLevel = queryParameters.get("priorityLevel")
                .map(Integer::parseInt)
                .orElse(0);
        this.maxRedeliverCount = queryParameters.get("maxRedeliverCount")
                .map(Integer::parseInt)
                .orElse(0);
        this.deadLetterTopic = queryParameters.get("deadLetterTopic")
                .orElse(topic + "-" + subscription + "-DLQ");
        this.pullMode = queryParameters.get("pullMode")
                .map(Boolean::parseBoolean)
                .orElse(false);
        this.negativeAckRedeliveryDelay = queryParameters.get("negativeAckRedeliveryDelay")
                .map(Integer::parseInt)
                .orElse(60000);
        this.token = queryParameters.get("token").orElse(null);
        this.redisKey = RedisMessaging.toRedisKey(tenant, namespace, topic);

        if (pullMode) {
            permitMessages.set(0);
        } else {
            permitMessages.set(receiverQueueSize);
        }

        LOG.trace("XGROUP CREATE {} {} {} MKSTREAM", redisKey, subscription, "$");
        redis.executeAsync(
                CommandType.XGROUP,
                new CommandArgs<>(Redis.CODEC)
                        .add("CREATE")
                        .add(redisKey)
                        .add(subscription)
                        .add("$")
                        .add("MKSTREAM"),
                new ReplayOutput<>()).whenComplete((r, e) -> {
            if (e != null) {
                if (!e.getMessage().startsWith("BUSYGROUP ")) {
                    LOG.error(e.getMessage(), e);
                    return;
                }
            }
            consume();
            redisKeyspaceNotification.addListener(redisKey, this);
        });
    }

    @OnMessage
    public void onMessage(String text) throws Exception {
        LOG.trace("{} Message: consumer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, text);

        JsonNode message = objectMapper.readTree(text);
        String type = message.path("type").textValue();
        if (type == null) {
            // acknowledge
            String messageId = message.path(RedisMessaging.MESSAGE_ID).textValue();

            LOG.trace("XACK {} {} {}", redisKey, subscription, messageId);
            redis.executeAsync(
                            CommandType.XACK,
                            new CommandArgs<>(Redis.CODEC)
                                    .add(redisKey)
                                    .add(subscription)
                                    .add(messageId),
                            new IntegerOutput<>(Redis.CODEC))
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            LOG.error(e.getMessage(), e);
                        } else {
                            if (!pullMode) {
                                permitMessages.addAndGet(r.intValue());
                                consume();
                            }
                        }
                    });
        } else {
            switch (type) {
                case "permit": {
                    if (pullMode) {
                        permitMessages.addAndGet(message.path("permitMessages").intValue());
                        consume();
                    }
                    break;
                }
                case "isEndOfTopic": {
                    session.getAsyncRemote().sendText(
                            objectMapper.createObjectNode()
                                    .put("endOfTopic", false)
                                    .toString()
                    );
                    break;
                }
                default: {
                    // negativeAcknowledge
                    LOG.error(text);
                    break;
                }
            }
        }
    }


    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: consumer/{}/{}/{} {}", session.getId(), tenant, namespace, topic, closeReason.getReasonPhrase());

        redisKeyspaceNotification.removeListener(redisKey, this);
    }

    @Override
    public void onKeyEvent(RedisKeyEvent event) {
        if (event.is("xadd")) {
            consume();
        }
    }

    protected void consume() {
        int count = permitMessages.get();
        if (count <= 0) {
            return;
        }
        if (lock.compareAndSet(false, true)) {
            consumeMore.set(false);
            LOG.trace("XREADGROUP GROUP {} {} COUNT {} STREAMS {} {}", subscription, consumerName, count, redisKey, ">");
            redis.executeAsync(
                            CommandType.XREADGROUP,
                            new CommandArgs<>(Redis.CODEC)
                                    .add("GROUP").add(subscription).add(consumerName)
                                    .add("COUNT").add(count)
                                    .add("STREAMS").add(redisKey).add(">"),
                            new RedisJsonMessageOutput(objectMapper, new ArrayList<>()))
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            LOG.error(e.getMessage(), e);
                        } else {
                            for (ObjectNode message : r) {
                                lastMessageId = message.path(RedisMessaging.MESSAGE_ID).textValue();
                                message.put(RedisMessaging.PUBLISH_TIME,
                                        RedisMessaging.format(RedisMessaging.getTime(lastMessageId), clock.getZone()));
                                message.put(RedisMessaging.REDELIVERY_COUNT, 0);

                                messageCounter.increment();
                                session.getAsyncRemote().sendText(message.toString());
                            }
                            permitMessages.addAndGet(-r.size());
                        }
                        lock.set(false);
                        if (consumeMore.get()) {
                            consume();
                        }
                    });
        } else {
            consumeMore.set(true);
        }
    }
}
