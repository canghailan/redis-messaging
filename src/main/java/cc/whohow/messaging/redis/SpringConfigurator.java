package cc.whohow.messaging.redis;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;

import javax.websocket.server.ServerEndpointConfig;

@Configuration
public class SpringConfigurator extends ServerEndpointConfig.Configurator implements ApplicationContextAware {
    static ApplicationContext applicationContext;

    public SpringConfigurator() {
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringConfigurator.applicationContext = applicationContext;
    }

    @Override
    public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
        return applicationContext.getBean(endpointClass);
    }
}
