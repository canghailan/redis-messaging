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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/pulsar/ws/v2/reader/persistent/{tenant}/{namespace}/{topic}", configurator = SpringConfigurator.class)
public class PulsarReader implements RedisKeyspaceListener {
    private static final String EARLIEST = "0";
    private static final String LATEST = "$";
    private static final Logger LOG = LoggerFactory.getLogger(PulsarReader.class);
    private final LongAdder messageCounter = new LongAdder();
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final AtomicBoolean readMore = new AtomicBoolean(true);

    @Inject
    private ZoneId timeZone;
    @Inject
    private ObjectMapper objectMapper;
    @Inject
    private Redis redis;
    @Inject
    private RedisKeyspaceNotification redisKeyspaceNotification;

    private String tenant;
    private String namespace;
    private String topic;
    private String readerName;
    private int receiverQueueSize;
    private String messageId;
    private String token;
    private String key;
    private Deque<String> pendingMessages;
    private String lastMessageId;
    private Session session;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("tenant") String tenant,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) {
        LOG.debug("{} Open: reader/{}/{}/{}", session.getId(), tenant, namespace, topic);

        QueryParameters queryParameters = new QueryParameters(session.getRequestParameterMap());

        this.tenant = tenant;
        this.namespace = namespace;
        this.topic = topic;
        this.readerName = queryParameters.get("readerName")
                .orElse(session.getId());
        this.receiverQueueSize = queryParameters.get("receiverQueueSize")
                .map(Integer::parseInt)
                .orElse(1000);
        this.messageId = queryParameters.get("messageId").orElse("latest");
        this.token = queryParameters.get("token").orElse(null);
        this.key = RedisMessaging.key(tenant, namespace, topic);
        this.pendingMessages = new LinkedBlockingDeque<>(receiverQueueSize);
        this.session = session;

        switch (messageId) {
            case "earliest" -> {
                lastMessageId = EARLIEST;
            }
            case "latest" -> {
                lastMessageId = LATEST;
            }
            default -> {
                lastMessageId = messageId;
            }
        }
        read();
        redisKeyspaceNotification.addListener(key, this);
    }

    @OnMessage
    public void onMessage(String text) throws Exception {
        LOG.trace("{} Message: reader/{}/{}/{} {}", session.getId(), tenant, namespace, topic, text);

        JsonNode message = objectMapper.readTree(text);
        String type = message.path("type").textValue();
        if (type == null) {
            // acknowledge
            String messageId = message.path("messageId").textValue();
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

        redisKeyspaceNotification.removeListener(key, this);
    }

    @Override
    public void onKeyEvent() {
        read();
    }

    protected void read() {
        int count = receiverQueueSize - pendingMessages.size();
        if (count <= 0) {
            return;
        }
        if (lock.compareAndSet(false, true)) {
            readMore.set(false);
            LOG.trace("XREAD COUNT {} STREAMS {} {}", count, key, lastMessageId);
            redis.executeAsync(
                            CommandType.XREAD,
                            new CommandArgs<>(Redis.CODEC)
                                    .add("COUNT").add(count)
                                    .add("STREAMS").add(key).add(lastMessageId),
                            new RedisMessageOutput<>(objectMapper, new ArrayList<>(count)))
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            LOG.error(e.getMessage(), e);
                        } else {
                            for (ObjectNode message : r) {
                                lastMessageId = message.path("messageId").textValue();
                                message.put("publishTime", RedisMessaging.format(RedisMessaging.getTime(lastMessageId), timeZone));
                                message.put("redeliveryCount", 0);

                                pendingMessages.addLast(lastMessageId);
                                messageCounter.increment();
                                session.getAsyncRemote().sendText(message.toString());
                            }
                            if (LATEST.equals(lastMessageId)) {
                                lastMessageId = EARLIEST;
                            }
                            lock.set(false);
                            if (readMore.get()) {
                                read();
                            }
                        }
                    });
        } else {
            readMore.set(true);
        }
    }
}
