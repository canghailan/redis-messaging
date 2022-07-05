package cc.whohow.messaging.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;
import java.util.List;

public class RedisJsonMessageOutput extends RedisMessageOutput<ObjectNode> {
    private final JsonNodeFactory jsonFactory;
    private ObjectNode message;
    private ObjectNode properties;

    public RedisJsonMessageOutput(ObjectMapper objectMapper, List<ObjectNode> output) {
        this(objectMapper.getNodeFactory(), output);
    }

    public RedisJsonMessageOutput(JsonNodeFactory jsonFactory, List<ObjectNode> output) {
        super(output);
        this.jsonFactory = jsonFactory;
        this.message = jsonFactory.objectNode();
        this.properties = jsonFactory.objectNode();
    }

    @Override
    protected void setMessageId(String messageId) {
        message.put(RedisMessaging.MESSAGE_ID, messageId);
    }

    @Override
    protected void setPayload(ByteBuffer payload) {
        message.put(RedisMessaging.PAYLOAD, base64(payload));
    }

    @Override
    protected void setMetadata(String key, String value) {
        message.put(key, value);
    }

    @Override
    protected void setProperties(String key, String value) {
        properties.put(key.substring(RedisMessaging.PROPERTIES_PREFIX.length()), value);
    }

    @Override
    protected void endMessage() {
        if (!properties.isEmpty()) {
            message.set(RedisMessaging.PROPERTIES, properties);
            properties = jsonFactory.objectNode();
        }
        output.add(message);
        message = jsonFactory.objectNode();
    }
}
