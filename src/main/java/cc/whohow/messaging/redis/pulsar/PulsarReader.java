package cc.whohow.messaging.redis.pulsar;

import cc.whohow.messaging.redis.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/pulsar/ws/v2/reader/persistent/{tenant}/{namespace}/{topic}", configurator = SpringConfigurator.class)
public class PulsarReader implements RedisKeyspaceListener {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarReader.class);
    private final LongAdder messageCounter = new LongAdder();
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final AtomicBoolean readMore = new AtomicBoolean(true);

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
    private String readerName;
    private int receiverQueueSize;
    private String messageId;
    private String token;
    private String redisKey;
    private Deque<String> pendingMessages;
    private String lastMessageId;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("tenant") String tenant,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) {
        LOG.debug("{} Open: reader/{}/{}/{}", session.getId(), tenant, namespace, topic);

        QueryParameters queryParameters = new QueryParameters(session.getRequestParameterMap());

        this.session = session;
        this.tenant = tenant;
        this.namespace = namespace;
        this.topic = topic;
        this.readerName = queryParameters.get("readerName").orElse(null);
        this.receiverQueueSize = queryParameters.get("receiverQueueSize")
                .map(Integer::parseInt)
                .orElse(1000);
        this.messageId = queryParameters.get("messageId").orElse("latest");
        this.token = queryParameters.get("token").orElse(null);
        this.redisKey = RedisMessaging.toRedisKey(tenant, namespace, topic);
        this.pendingMessages = new LinkedBlockingDeque<>(receiverQueueSize);

        switch (messageId) {
            case "earliest": {
                lastMessageId = RedisMessaging.EARLIEST;
                break;
            }
            case "latest": {
                lastMessageId = Long.toString(clock.millis());
                break;
            }
            default: {
                if (messageId.startsWith(RedisMessaging.TIMESTAMP_PREFIX)) {
                    lastMessageId = messageId.substring(1);
                } else {
                    lastMessageId = messageId;
                }
                break;
            }
        }
        read();
        redisKeyspaceNotification.addListener(redisKey, this);
    }

    @OnMessage
    public void onMessage(String text) throws Exception {
        LOG.trace("{} Message: reader/{}/{}/{} {}", session.getId(), tenant, namespace, topic, text);

        JsonNode message = objectMapper.readTree(text);
        String type = message.path("type").textValue();
        if (type == null) {
            // acknowledge
            String messageId = message.path(RedisMessaging.MESSAGE_ID).textValue();
            pendingMessages.remove(messageId);
            read();
        } else if ("isEndOfTopic".equals(type)) {
            session.getAsyncRemote().sendText(
                    objectMapper.createObjectNode()
                            .put("endOfTopic", false)
                            .toString()
            );
        } else {
            LOG.error(text);
        }
    }

    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: reader/{}/{}/{} {}", session.getId(), tenant, namespace, topic, closeReason.getReasonPhrase());

        redisKeyspaceNotification.removeListener(redisKey, this);
    }

    @Override
    public void onKeyEvent(RedisKeyEvent event) {
        if (event.is("xadd")) {
            read();
        }
    }

    protected void read() {
        int count = receiverQueueSize - pendingMessages.size();
        if (count <= 0) {
            return;
        }
        if (lock.compareAndSet(false, true)) {
            readMore.set(false);
            LOG.trace("XREAD COUNT {} STREAMS {} {}", count, redisKey, lastMessageId);
            redis.executeAsync(
                            CommandType.XREAD,
                            new CommandArgs<>(Redis.CODEC)
                                    .add("COUNT").add(count)
                                    .add("STREAMS").add(redisKey).add(lastMessageId),
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

                                pendingMessages.addLast(lastMessageId);
                                messageCounter.increment();
                                session.getAsyncRemote().sendText(message.toString());
                            }
                            if (RedisMessaging.LATEST.equals(lastMessageId)) {
                                lastMessageId = RedisMessaging.EARLIEST;
                            }
                            lock.set(false);
                            if (readMore.get() || r.size() == count) {
                                read();
                            }
                        }
                    });
        } else {
            readMore.set(true);
        }
    }
}
