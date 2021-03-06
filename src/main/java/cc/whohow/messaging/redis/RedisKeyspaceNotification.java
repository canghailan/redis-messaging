package cc.whohow.messaging.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class RedisKeyspaceNotification implements RedisPubSubListener<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyspaceNotification.class);

    protected final Map<String, List<RedisKeyspaceListener>> listeners = new ConcurrentHashMap<>();
    protected final StatefulRedisPubSubConnection<String, String> connection;
    protected final String keyEventPrefix;

    public RedisKeyspaceNotification(RedisClient redisClient, RedisURI uri) {
        this.connection = redisClient.connectPubSub(uri);
        this.keyEventPrefix = "__keyevent@" + uri.getDatabase() + "__:";

        this.connection.addListener(this);
        this.connection.async().psubscribe(keyEventPrefix + "*");
    }

    public void addListener(String key, RedisKeyspaceListener listener) {
        listeners.computeIfAbsent(key, (k) -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public void removeListener(String key, RedisKeyspaceListener listener) {
        List<RedisKeyspaceListener> list = listeners.get(key);
        list.remove(listener);
        if (list.isEmpty()) {
            listeners.values().removeIf(List::isEmpty);
        }
    }

    @Override
    public void message(String channel, String message) {
    }

    @Override
    public void message(String pattern, String channel, String message) {
        LOG.trace("{} {}", channel, message);
        List<RedisKeyspaceListener> list = listeners.get(message);
        if (list != null) {
            RedisKeyEvent event = new RedisKeyEvent(channel, message);
            for (RedisKeyspaceListener listener : list) {
                try {
                    listener.onKeyEvent(event);
                } catch (Throwable e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void subscribed(String channel, long count) {

    }

    @Override
    public void psubscribed(String pattern, long count) {
        LOG.trace("psubscribed {}", pattern);
    }

    @Override
    public void unsubscribed(String channel, long count) {

    }

    @Override
    public void punsubscribed(String pattern, long count) {
        LOG.trace("punsubscribed {}", pattern);
    }
}
