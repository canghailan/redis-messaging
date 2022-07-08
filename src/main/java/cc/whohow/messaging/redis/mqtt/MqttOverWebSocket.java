package cc.whohow.messaging.redis.mqtt;

import cc.whohow.messaging.redis.SpringConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Named;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.nio.ByteBuffer;

@Named
@Scope("prototype")
@ServerEndpoint(value = "/mqtt", subprotocols = "mqtt", configurator = SpringConfigurator.class)
public class MqttOverWebSocket {
    private static final Logger LOG = LoggerFactory.getLogger(MqttOverWebSocket.class);
    private Session session;

    @OnOpen
    public void onOpen(Session session) {
        LOG.debug("{} Open: mqtt", session.getId());

        this.session = session;
    }

    @OnMessage
    public void onMessage(ByteBuffer bytes) throws Exception {
        LOG.debug("{} Message: mqtt", session.getId());

        LOG.trace("{}", bytes);
    }

    @OnClose
    public void onClose(CloseReason closeReason) {
        LOG.debug("{} Close: mqtt {}", session.getId(), closeReason.getReasonPhrase());
    }
}
