package cc.whohow.messaging.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.undertow.Undertow;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.websockets.jsr.WebSocketDeploymentInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Properties;

@Configuration
public class RedisMessagingServerConfiguration {
    @Inject
    ApplicationContext applicationContext;

    @Bean
    public ZoneId timeZone() {
        return ZoneId.systemDefault();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public RedisURI redisURI() {
        try (Reader reader = Files.newBufferedReader(Paths.get("redis.properties"))) {
            Properties properties = new Properties();
            properties.load(reader);
            return RedisURI.create(properties.getProperty("uri"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Bean
    public RedisClient redisClient() {
        return RedisClient.create();
    }

    @Bean
    public Redis redis(@Autowired RedisClient redisClient, @Autowired RedisURI redisURI) {
        return new StandaloneRedis(redisClient, redisURI);
    }

    @Bean
    public RedisKeyspaceNotification redisKeyspaceNotification(@Autowired RedisClient redisClient, @Autowired RedisURI redisURI) {
        return new RedisKeyspaceNotification(redisClient, redisURI);
    }

    @Bean
    public WebSocketDeploymentInfo webSocketDeploymentInfo() {
        WebSocketDeploymentInfo webSockets = new WebSocketDeploymentInfo();
        for (Object endpoint : applicationContext.getBeansWithAnnotation(ServerEndpoint.class).values()) {
            webSockets.addEndpoint(endpoint.getClass());
        }
        return webSockets;
    }

    @Bean
    public DeploymentInfo deploymentInfo(@Autowired WebSocketDeploymentInfo webSockets) {
        return Servlets.deployment()
                .setContextPath("/")
                .setClassLoader(Thread.currentThread().getContextClassLoader())
                .setDeploymentName("redis-messaging")
                .addServletContextAttribute(WebSocketDeploymentInfo.ATTRIBUTE_NAME, webSockets);
    }

    @Bean
    public Undertow undertow(@Autowired DeploymentInfo deployment) throws Exception {
        DeploymentManager deploymentManager = Servlets.defaultContainer()
                .addDeployment(deployment);
        deploymentManager.deploy();

        return Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(deploymentManager.start())
                .build();
    }
}
