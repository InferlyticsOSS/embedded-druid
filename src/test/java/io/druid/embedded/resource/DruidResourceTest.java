package io.druid.embedded.resource;

import io.druid.data.input.Row;
import io.druid.embedded.QueryHelper;
import io.druid.embedded.app.DruidRunner;
import io.druid.embedded.helper.DruidClient;
import io.druid.embedded.helper.IndexCreationHelper;
import io.druid.embedded.helper.QueryCreationHelper;
import io.druid.query.Query;
import io.druid.query.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Tests querying the server
 */
public class DruidResourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(DruidResourceTest.class);
    private static final int PORT;
    private static final DruidRunner druidRunner;
    private static final DruidClient client;

    static {
        PORT = 2000 + new Random().nextInt(60000);
        client = new DruidClient("http://localhost:" + PORT + "/druid/v2");
        try {
            druidRunner = new DruidRunner(PORT, IndexCreationHelper.createDruidSegments());
        } catch (IOException e) {
            throw new IllegalStateException("Could not create index", e);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        druidRunner.run();
    }

    @Test
    public void testTopNQuery() throws Exception {
        Query query = QueryCreationHelper.getTopNQuery();
        List<Result> results = client.topN(query);
        Assert.assertEquals(results.size(), 1);
    }

    @Test
    public void testGroupByQuery() throws Exception {
        Query query = QueryCreationHelper.getGroupByQuery();
        LOG.info(QueryHelper.jsonMapper.writeValueAsString(query));
        List<Row> results = client.groupBy(query);
        Assert.assertEquals(results.size(), 2);

        if (results.get(0).getDimension("URL").get(0).equals("abc")) {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 12);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 3);

        } else {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 3);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 12);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        druidRunner.stop();
    }
}
