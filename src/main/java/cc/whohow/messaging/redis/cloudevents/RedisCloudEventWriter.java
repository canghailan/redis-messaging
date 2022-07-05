package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.Redis;
import cc.whohow.messaging.redis.RedisMessaging;
import io.cloudevents.CloudEventData;
import io.cloudevents.rw.CloudEventContextWriter;
import io.cloudevents.rw.CloudEventRWException;
import io.cloudevents.rw.CloudEventWriter;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RedisCloudEventWriter implements CloudEventWriter<CompletableFuture<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisCloudEventWriter.class);
    protected final Redis redis;
    protected CommandArgs<byte[], byte[]> commandArgs = new CommandArgs<>(Redis.CODEC);

    public RedisCloudEventWriter(Redis redis, String key) {
        this.redis = redis;
        this.commandArgs.add(key).add("*");
    }

    @Override
    public CompletableFuture<String> end(CloudEventData data) throws CloudEventRWException {
        commandArgs.add(RedisMessaging.PAYLOAD).add(data.toBytes());
        return end();
    }

    @Override
    public CompletableFuture<String> end() throws CloudEventRWException {
        CommandOutput<byte[], byte[], List<String>> commandOutput = new StringListOutput<>(Redis.CODEC);
        commandOutput.multi(1);

        if (LOG.isTraceEnabled()) {
            LOG.trace("XADD {}", commandArgs.toCommandString());
        }
        return redis.executeAsync(CommandType.XADD, commandArgs, commandOutput)
                .thenApply(list -> list.get(0));
    }

    @Override
    public CloudEventContextWriter withContextAttribute(String name, String value) throws CloudEventRWException {
        commandArgs.add(RedisMessaging.CLOUDEVENTS_PROPERTIES_PREFIX + name).add(value);
        return this;
    }
}
