package cc.whohow.messaging.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.jackson.JsonFormat;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.websockets.jsr.WebSocketDeploymentInfo;
import org.jboss.resteasy.core.ResteasyDeploymentImpl;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;
import javax.websocket.server.ServerEndpoint;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Properties;

@Configuration
public class RedisMessagingServerConfiguration {
    @Inject
    ApplicationContext applicationContext;

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
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
    public JsonFormat jsonFormat() {
        return new JsonFormat();
    }

    @Bean
    public ResteasyDeployment resteasyDeployment(@Autowired Application application) {
        ResteasyDeployment resteasyDeployment = new ResteasyDeploymentImpl();
        resteasyDeployment.setApplication(application);
        return resteasyDeployment;
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
    public UndertowJaxrsServer server(
            @Autowired ResteasyDeployment resteasyDeployment,
            @Autowired WebSocketDeploymentInfo webSockets) throws Exception {
        UndertowJaxrsServer jaxrsServer = new UndertowJaxrsServer();
        jaxrsServer.setPort(8080);

        DeploymentInfo deploymentInfo = jaxrsServer.undertowDeployment(resteasyDeployment)
                .setContextPath("/")
                .setClassLoader(Thread.currentThread().getContextClassLoader())
                .setDeploymentName("redis-messaging")
                .addServletContextAttribute(WebSocketDeploymentInfo.ATTRIBUTE_NAME, webSockets);
        jaxrsServer.deploy(deploymentInfo);

        return jaxrsServer;
    }
}
