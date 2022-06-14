package cc.whohow.messaging.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.lettuce.core.output.CommandOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
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
public class RedisMessageOutput<T> extends CommandOutput<byte[], byte[], List<ObjectNode>> {
    private static final int STREAM = 2;
    private static final int ID = 4;
    private static final int KEY_VALUE = 5;
    private final ObjectMapper objectMapper;
    private int depth = 0;
    private String key;
    private ObjectNode message;

    public RedisMessageOutput(ObjectMapper objectMapper, List<ObjectNode> output) {
        super(Redis.CODEC, output);
        this.objectMapper = objectMapper;
        this.message = objectMapper.createObjectNode();
    }

    @Override
    public void set(ByteBuffer bytes) {
        switch (depth) {
            case STREAM -> {
            }
            case ID -> {
                message.put("messageId", decodeAscii(bytes));
            }
            case KEY_VALUE -> {
                if (key == null) {
                    key = decodeAscii(bytes);
                } else {
                    switch (key) {
                        case "payload" -> {
                            message.put("payload", base64(bytes));
                        }
                        case "properties" -> {
                            message.set("properties", json(bytes));
                        }
                    }
                    key = null;
                }
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
            output.add(message);
            message = objectMapper.createObjectNode();
        }
        this.depth = depth;
    }

    public String base64(ByteBuffer bytes) {
        return StandardCharsets.US_ASCII.decode(Base64.getEncoder().encode(bytes)).toString();
    }

    public JsonNode json(ByteBuffer bytes) {
        try {
            return objectMapper.readTree(new ByteBufferBackedInputStream(bytes));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
