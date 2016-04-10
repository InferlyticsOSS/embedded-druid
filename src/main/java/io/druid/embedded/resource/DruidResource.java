package io.druid.embedded.resource;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.embedded.QueryHelper;
import io.druid.embedded.jackson.QueryObjectMapper;
import io.druid.embedded.service.DruidService;
import io.swagger.annotations.Api;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Endpoint for queries
 */
@Path("/v2")
@Api("Druid API")
public class DruidResource {
    private static final Logger LOG = Logger.getLogger(DruidResource.class);

    /**
     * Used as a response in case of failure
     */
    private class FailureResponse {
        private String message;

        public FailureResponse(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(@Context HttpServletRequest req, String queryJson) {
        try {
            int indexKey = req.getServerPort();
            return Response.ok(QueryHelper.jsonMapper.writeValueAsString(DruidService.handleQuery(indexKey, queryJson))).build();
        } catch (JsonMappingException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(new FailureResponse(e.getMessage())).build();
        } catch (Exception e) {
            LOG.error("Exception while handling query", e);
            return Response.serverError().entity(new FailureResponse("Internal Server Error")).build();
        }
    }
}
