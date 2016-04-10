package io.druid.embedded.service;

import com.metamx.common.guava.Sequence;
import io.druid.embedded.QueryHelper;
import io.druid.embedded.app.DruidRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Handles queries from DruidResource and returns the response
 */
public class DruidService {
    private static final Logger LOG = Logger.getLogger(DruidService.class);

    public static Sequence handleQuery(Integer indexKey, String queryJson) throws IOException {
        LOG.trace("Got query: " + queryJson);
        return QueryHelper.run(queryJson, DruidRunner.INDEX_MAP.get(indexKey));
    }
}
