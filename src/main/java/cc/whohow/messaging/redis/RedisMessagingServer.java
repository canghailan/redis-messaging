package cc.whohow.messaging.redis;

import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan
public class RedisMessagingServer {
    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(RedisMessagingServer.class)
                .getBean(UndertowJaxrsServer.class)
                .start();
    }
}
