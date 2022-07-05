package cc.whohow.messaging.redis;

public class RedisKeyEvent {
    private final String channel;
    private final String key;

    public RedisKeyEvent(String channel, String key) {
        this.channel = channel;
        this.key = key;
    }

    public boolean is(String command) {
        return channel.endsWith(command) &&
                channel.length() > command.length() &&
                channel.charAt(channel.length() - command.length() - 1) == ':';
    }

    public String getCommand() {
        return channel.substring(channel.lastIndexOf(':'));
    }

    public String getKey() {
        return key;
    }
}
