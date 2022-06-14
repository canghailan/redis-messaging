package cc.whohow.messaging.redis;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class RedisMessaging {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ");

    public static long getTime(String messageId) {
        return Long.parseLong(messageId.substring(0, messageId.indexOf('-')));
    }

    public static String format(long timestamp, ZoneId timeZone) {
        return DATE_TIME_FORMATTER.format(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone));
    }

    public static String key(String tenant, String namespace, String topic) {
        return namespace + ":" + topic;
    }
}
