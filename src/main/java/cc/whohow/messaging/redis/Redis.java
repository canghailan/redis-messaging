package cc.whohow.messaging.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.util.concurrent.CompletableFuture;

public interface Redis extends AutoCloseable {
    RedisCodec<byte[], byte[]> CODEC = RedisCodec.of(ByteArrayCodec.INSTANCE, ByteArrayCodec.INSTANCE);

    boolean isOpen();

    <T> CompletableFuture<T> executeAsync(ProtocolKeyword type, CommandOutput<byte[], byte[], T> output);

    <T> CompletableFuture<T> executeAsync(ProtocolKeyword type, CommandArgs<byte[], byte[]> args, CommandOutput<byte[], byte[], T> output);

    CompletableFuture<Void> closeAsync();
}
