package cc.whohow.messaging.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.util.concurrent.CompletableFuture;

public class RedisImpl implements Redis {
    protected final StatefulRedisConnection<byte[], byte[]> connection;

    public RedisImpl(RedisClient client, RedisURI uri) {
        this.connection = client.connect(CODEC, uri);
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(ProtocolKeyword type, CommandOutput<byte[], byte[], T> output) {
        AsyncCommand<byte[], byte[], T> command = new AsyncCommand<>(new Command<>(type, output));
        connection.dispatch(command);
        return command;
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(ProtocolKeyword type, CommandArgs<byte[], byte[]> args, CommandOutput<byte[], byte[], T> output) {
        AsyncCommand<byte[], byte[], T> command = new AsyncCommand<>(new Command<>(type, output, args));
        connection.dispatch(command);
        return command;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return connection.closeAsync();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }
}
