package cc.whohow.messaging.redis;

import io.undertow.Undertow;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan
public class RedisMessagingServer {
    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(RedisMessagingServer.class)
                .getBean(Undertow.class)
                .start();
    }
}
