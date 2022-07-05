package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.Redis;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

@Named
@Path("cloudevents")
public class CloudEventsHttp {
    @Inject
    private Redis redis;

    @POST
    @Path("{topic}")
    public CompletionStage<Response> post(@PathParam("topic") String topic,
                                          @HeaderParam("Authorization") String authorization,
                                          CloudEvent cloudEvent) {
        return CloudEventUtils.toReader(cloudEvent).read(new RedisCloudEventWriterFactory(redis, topic))
                .handle((r, e) -> {
                    if (e != null) {
                        return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage())
                                .build();
                    } else {
                        return Response.ok().build();
                    }
                });
    }
}
