package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.RedisMessageOutput;
import cc.whohow.messaging.redis.RedisMessaging;
import cc.whohow.messaging.redis.pulsar.PulsarProducer;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class RedisCloudEventOutput extends RedisMessageOutput<CloudEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarProducer.class);

    protected CloudEventBuilder cloudEventBuilder;
    protected String messageId;

    public RedisCloudEventOutput(List<CloudEvent> output) {
        super(output);
    }

    @Override
    protected void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @Override
    protected void setPayload(ByteBuffer payload) {
        cloudEventBuilder.withData(bytes(payload));
    }

    @Override
    protected void setMetadata(String key, String value) {
        LOG.warn("{}: {}", key, value);
    }

    @Override
    protected void setProperties(String key, String value) {
        if (key.startsWith(RedisMessaging.CLOUDEVENTS_PROPERTIES_PREFIX)) {
            String attribute = key.substring(RedisMessaging.CLOUDEVENTS_PROPERTIES_PREFIX.length());
            if (CloudEventV1.SPECVERSION.equals(attribute)) {
                cloudEventBuilder = CloudEventBuilder.fromSpecVersion(SpecVersion.parse(value));
            } else {
                cloudEventBuilder.withContextAttribute(attribute, value);
            }
        } else {
            LOG.warn("{}: {}", key, value);
        }
    }

    @Override
    protected void endMessage() {
        cloudEventBuilder.withContextAttribute(RedisMessaging.CLOUDEVENTS_MESSAGE_ID, messageId);
        output.add(cloudEventBuilder.build());
        cloudEventBuilder = null;
    }
}
