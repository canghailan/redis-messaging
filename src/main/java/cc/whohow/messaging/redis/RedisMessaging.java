package cc.whohow.messaging.redis;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class RedisMessaging {
    public static final String EARLIEST = "0";
    public static final String LATEST = "$";
    public static final String MESSAGE_ID = "messageId";
    public static final String PAYLOAD = "payload";
    public static final String PROPERTIES = "properties";
    public static final String PROPERTIES_PREFIX = PROPERTIES + ".";
    public static final String CONTEXT = "context";
    public static final String KEY = "key";
    public static final String REPLICATION_CLUSTERS = "replicationClusters";
    public static final String PRODUCER_NAME = "producerName";
    public static final String PUBLISH_TIME = "publishTime";
    public static final String REDELIVERY_COUNT = "redeliveryCount";
    public static final String TIMESTAMP_PREFIX = "@";
    public static final String CLOUDEVENTS_MESSAGE_ID = MESSAGE_ID.toLowerCase();
    public static final String CLOUDEVENTS_PROPERTIES_PREFIX = PROPERTIES + ".ce-";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ");

    public static long getTime(String messageId) {
        return Long.parseLong(messageId.substring(0, messageId.indexOf('-')));
    }

    public static String format(long timestamp, ZoneId timeZone) {
        return DATE_TIME_FORMATTER.format(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone));
    }

    public static String toRedisKey(String tenant, String namespace, String topic) {
        return namespace + ":" + topic;
    }
}
