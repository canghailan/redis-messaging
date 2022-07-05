package cc.whohow.messaging.redis;

public interface RedisKeyspaceListener {
    void onKeyEvent(RedisKeyEvent event);
}
