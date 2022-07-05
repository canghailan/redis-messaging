package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.Redis;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.rw.CloudEventRWException;
import io.cloudevents.rw.CloudEventWriterFactory;

import java.util.concurrent.CompletableFuture;

public class RedisCloudEventWriterFactory implements CloudEventWriterFactory<RedisCloudEventWriter, CompletableFuture<String>> {
    protected final Redis redis;
    protected final String key;

    public RedisCloudEventWriterFactory(Redis redis, String key) {
        this.redis = redis;
        this.key = key;
    }

    @Override
    public RedisCloudEventWriter create(SpecVersion version) throws CloudEventRWException {
        RedisCloudEventWriter writer = new RedisCloudEventWriter(redis, key);
        writer.withContextAttribute(CloudEventV1.SPECVERSION, version.toString());
        return writer;
    }
}
