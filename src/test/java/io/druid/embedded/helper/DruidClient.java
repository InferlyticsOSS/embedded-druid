package io.druid.embedded.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.druid.data.input.Row;
import io.druid.embedded.QueryHelper;
import io.druid.query.Query;
import io.druid.query.Result;
import retrofit.RestAdapter;
import retrofit.converter.JacksonConverter;
import retrofit.http.Body;
import retrofit.http.POST;

import java.util.List;

/**
 * Acts as a client to make calls to Druid
 */
public class DruidClient {
    private interface Druid {
        @POST("/")
        List<Result> topN(@Body Query query);

        @POST("/")
        List<Row> groupBy(@Body Query query);
    }

    private Druid druid;

    public DruidClient(String apiUrl) {
        druid = new RestAdapter.Builder().setEndpoint(apiUrl).setConverter(new JacksonConverter(QueryHelper.jsonMapper))
                .build().create(Druid.class);
    }

    public List<Result> topN(Query query) throws JsonProcessingException {
        return druid.topN(query);
    }

    public List<Row> groupBy(Query query) throws JsonProcessingException {
        return druid.groupBy(query);
    }
}
