package cc.whohow.messaging.redis.cloudevents;

import cc.whohow.messaging.redis.Redis;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

@Named
@Path("cloudevents/webhook")
public class CloudEventsWebHook {
    @Inject
    private Redis redis;

    @POST
    @Path("{topic}")
    public CompletionStage<Response> webhook(@PathParam("topic") String topic,
                                             @HeaderParam("Authorization") String authorization,
                                             @HeaderParam("WebHook-Request-Origin") String webhookRequestOrigin,
                                             @HeaderParam("WebHook-Request-Callback") String webhookRequestCallback,
                                             @HeaderParam("WebHook-Request-Rate") double webhookRequestRate,
                                             @QueryParam("access_token") String accessToken,
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
