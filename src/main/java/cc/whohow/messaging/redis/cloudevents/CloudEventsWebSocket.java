package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.*;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;
import io.cloudevents.jackson.JsonFormat;
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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/cloudevents/{protocol}/{topic}", configurator = SpringConfigurator.class)
public class CloudEventsWebSocket implements RedisKeyspaceListener {
    private static final Logger LOG = LoggerFactory.getLogger(CloudEventsWebSocket.class);
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final AtomicBoolean readMore = new AtomicBoolean(true);
    private final int count = 100;
    @Inject
    private Clock clock;
    @Inject
    private JsonFormat jsonFormat;
    @Inject
    private Redis redis;
    @Inject
    private RedisKeyspaceNotification redisKeyspaceNotification;
    private Session session;
    private String protocol;
    private String topic;
    private String lastMessageId;
    private RedisCloudEventWriterFactory writerFactory;
    private boolean sub;

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("protocol") String protocol,
                       @PathParam("topic") String topic) throws Exception {
        LOG.debug("{} Open: cloudevents/{}/{}", session.getId(), protocol, topic);

        this.session = session;
        this.protocol = protocol;
        this.topic = topic;
        this.lastMessageId = Long.toString(clock.millis());
        this.writerFactory = new RedisCloudEventWriterFactory(redis, topic);
        switch (protocol) {
            case "ws": {
                this.sub = false;
                break;
            }
            case "pubsub": {
                this.sub = true;
                break;
            }
            default: {
                session.close(new CloseReason(CloseReason.CloseCodes.CLOSED_ABNORMALLY,
                        protocol + " not unsupported"));
                break;
            }
        }

        if (sub) {
            redisKeyspaceNotification.addListener(topic, this);
        }
    }

    @OnMessage
    public void onMessage(String text) {
        LOG.trace("{} Message: cloudevents/{}/{} {}", session.getId(), protocol, topic, text);

        CloudEvent cloudEvent = jsonFormat.deserialize(text.getBytes(StandardCharsets.UTF_8));
        CloudEventUtils.toReader(cloudEvent).read(writerFactory).whenComplete((r, e) -> {
            if (e != null) {
                LOG.error(cloudEvent.getId() + " Error", e.getMessage());
            } else {
                LOG.trace("{} Ok: {}", cloudEvent.getId(), r);
            }
        });
    }

    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: cloudevents/{}/{} {}", session.getId(), protocol, topic, closeReason.getReasonPhrase());

        if (sub) {
            redisKeyspaceNotification.removeListener(topic, this);
        }
    }

    @Override
    public void onKeyEvent(RedisKeyEvent event) {
        if (event.is("xadd")) {
            read();
        }
    }

    protected void read() {
        if (lock.compareAndSet(false, true)) {
            readMore.set(false);
            LOG.trace("XREAD COUNT {} STREAMS {} {}", count, topic, lastMessageId);
            redis.executeAsync(
                            CommandType.XREAD,
                            new CommandArgs<>(Redis.CODEC)
                                    .add("COUNT").add(count)
                                    .add("STREAMS").add(topic).add(lastMessageId),
                            new RedisCloudEventOutput(new ArrayList<>()))
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            LOG.error(e.getMessage(), e);
                        } else {
                            for (CloudEvent cloudEvent : r) {
                                lastMessageId = (String) cloudEvent.getExtension(RedisMessaging.CLOUDEVENTS_MESSAGE_ID);
                                session.getAsyncRemote().sendText(
                                        new String(jsonFormat.serialize(cloudEvent), StandardCharsets.UTF_8));
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
