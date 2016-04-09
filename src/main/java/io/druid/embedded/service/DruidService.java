package io.druid.embedded.service;

import com.metamx.common.guava.Sequence;
import io.druid.embedded.QueryHelper;
import io.druid.embedded.app.DruidRunner;
import io.druid.embedded.exception.UnhandledQueryException;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by srira on 4/7/2016.
 */
public class DruidService {
    private static final Logger LOG = Logger.getLogger(DruidService.class);

    public static Sequence handleQuery(Integer indexKey, String queryJson) throws UnhandledQueryException, IOException {
        return QueryHelper.run(queryJson, DruidRunner.INDEX_MAP.get(indexKey));
    }
}
