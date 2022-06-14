package cc.whohow.messaging.redis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryParameters {
    protected final Map<String, List<String>> parameterMap;

    public QueryParameters(Map<String, List<String>> parameterMap) {
        this.parameterMap = parameterMap;
    }

    public Optional<String> get(String key) {
        List<String> parameter = parameterMap.get(key);
        if (parameter == null || parameter.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(parameter.get(0));
    }
}
