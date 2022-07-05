package cc.whohow.messaging.redis;

import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

// > XREAD COUNT 2 STREAMS mystream writers 0-0 0-0
//1) 1) "mystream"
//   2) 1) 1) 1526984818136-0
//         2) 1) "duration"
//            2) "1532"
//            3) "event-id"
//            4) "5"
//            5) "user-id"
//            6) "7782813"
//      2) 1) 1526999352406-0
//         2) 1) "duration"
//            2) "812"
//            3) "event-id"
//            4) "9"
//            5) "user-id"
//            6) "388234"
//2) 1) "writers"
//   2) 1) 1) 1526985676425-0
//         2) 1) "name"
//            2) "Virginia"
//            3) "surname"
//            4) "Woolf"
//      2) 1) 1526985685298-0
//         2) 1) "name"
//            2) "Jane"
//            3) "surname"
//            4) "Austen"
public abstract class RedisMessageOutput<T> extends CommandOutput<byte[], byte[], List<T>> {
    private static final int STREAM = 2;
    private static final int ID = 4;
    private static final int KEY_VALUE = 5;
    private int depth = 0;
    private String key;

    public RedisMessageOutput(List<T> output) {
        super(Redis.CODEC, output);
    }

    @Override
    public void set(ByteBuffer bytes) {
        switch (depth) {
            case STREAM: {
                break;
            }
            case ID: {
                setMessageId(decodeAscii(bytes));
                break;
            }
            case KEY_VALUE: {
                if (key == null) {
                    key = decodeAscii(bytes);
                } else {
                    if (RedisMessaging.PAYLOAD.equals(key)) {
                        setPayload(bytes);
                    } else if (key.startsWith(RedisMessaging.PROPERTIES_PREFIX)) {
                        setProperties(key, utf8(bytes));
                    } else {
                        setMetadata(key, utf8(bytes));
                    }
                    key = null;
                }
                break;
            }
        }
    }

    @Override
    public void multiArray(int count) {
        depth++;
    }

    @Override
    public void complete(int depth) {
        if (this.depth == KEY_VALUE && depth == ID) {
            endMessage();
        }
        this.depth = depth;
    }

    public String utf8(ByteBuffer bytes) {
        return StandardCharsets.UTF_8.decode(bytes).toString();
    }

    public String base64(ByteBuffer bytes) {
        return StandardCharsets.US_ASCII.decode(Base64.getEncoder().encode(bytes)).toString();
    }

    public byte[] bytes(ByteBuffer bytes) {
        byte[] buffer = new byte[bytes.remaining()];
        bytes.get(buffer);
        return buffer;
    }

    protected abstract void setMessageId(String messageId);

    protected abstract void setPayload(ByteBuffer payload);

    protected abstract void setMetadata(String key, String value);

    protected abstract void setProperties(String key, String value);

    protected abstract void endMessage();
}
