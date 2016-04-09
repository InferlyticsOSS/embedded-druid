package io.druid.embedded.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.MissingAggregatorsModule;

/**
 * Adds support for missing Aggregator Factories
 */
public class QueryObjectMapper extends DefaultObjectMapper {
    private static final long serialVersionUID = 1231241241241L;

    public QueryObjectMapper() {
        this((JsonFactory)null);
    }

    public QueryObjectMapper(QueryObjectMapper mapper) {
        super(mapper);
    }

    public QueryObjectMapper(JsonFactory factory) {
        super(factory);
        registerModule(new MissingAggregatorsModule());
    }

    @Override
    public ObjectMapper copy() {
        return new QueryObjectMapper(this);
    }
}
