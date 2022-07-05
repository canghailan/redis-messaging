package cc.whohow.messaging.redis;

import org.springframework.context.ApplicationContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.Provider;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Named
public class JaxRsApplication extends Application {
    @Inject
    ApplicationContext context;

    @Override
    public Set<Object> getSingletons() {
        Map<String, Object> providers = context.getBeansWithAnnotation(Provider.class);
        Map<String, Object> resources = context.getBeansWithAnnotation(Path.class);

        Set<Object> set = new LinkedHashSet<>();
        set.addAll(providers.values());
        set.addAll(resources.values());
        return set;
    }
}
