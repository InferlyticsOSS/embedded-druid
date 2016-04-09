package io.druid.embedded.helper;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.embedded.IndexHelper;
import io.druid.embedded.load.Loader;
import io.druid.embedded.load.impl.CSVLoader;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by srira on 4/8/2016.
 */
public class IndexCreationHelper {
    public static QueryableIndex createDruidSegments() throws IOException {
        //  Create druid segments from raw data
        Reader reader = new BufferedReader(new FileReader(new File("./src/test/resources/report.csv")));

        List<String> columns = Arrays.asList("colo", "pool", "report", "URL", "TS", "metric", "value", "count", "min", "max", "sum");
        List<String> exclusions = Arrays.asList("_Timestamp", "_Machine", "_ThreadId", "_Query");
        List<String> metrics = Arrays.asList("value", "count", "min", "max", "sum");
        List<String> dimensions = new ArrayList<String>(columns);
        dimensions.removeAll(exclusions);
        dimensions.removeAll(metrics);
        Loader loader = new CSVLoader(reader, columns, dimensions, "TS");

        DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
        AggregatorFactory[] metricsAgg = new AggregatorFactory[]{
                new LongSumAggregatorFactory("agg_count", "count"),
                new MaxAggregatorFactory("agg_max", "max"),
                new MinAggregatorFactory("agg_min", "min"),
                new DoubleSumAggregatorFactory("agg_sum", "sum"),
                new ApproximateHistogramAggregatorFactory("agg_histogram", "value", null, null, null, null)
        };
        IncrementalIndexSchema indexSchema = new IncrementalIndexSchema(0, QueryGranularity.ALL, dimensionsSpec, metricsAgg);
        QueryableIndex index = IndexHelper.getQueryableIndex(loader, indexSchema);
        return index;
    }
}
