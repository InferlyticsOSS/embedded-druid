package io.druid.embedded.helper;

import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.aggregation.histogram.QuantilesPostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.QueryableIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by srira on 4/9/2016.
 */
public class QueryCreationHelper {
    public static Query getGroupByQuery() {
        List<DimFilter> filters = new ArrayList<DimFilter>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return GroupByQuery.builder()
                .setDataSource("test")
                .setQuerySegmentSpec(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .setGranularity(QueryGranularity.NONE)
                .addDimension("URL")
                .addAggregator(new LongSumAggregatorFactory("agg_count", "agg_count"))
                .addAggregator(new MaxAggregatorFactory("agg_max", "agg_max"))
                .addAggregator(new MinAggregatorFactory("agg_min", "agg_min"))
                .addAggregator(new DoubleSumAggregatorFactory("agg_sum", "agg_sum"))
                .addAggregator(new ApproximateHistogramFoldingAggregatorFactory("agg_histogram", "agg_histogram", 20, 5, null, null))
                .addPostAggregator(new QuantilesPostAggregator("agg_quantiles", "agg_histogram", new float[]{0.25f, 0.5f, 0.75f, 0.95f, 0.99f}))
                .setDimFilter(DimFilters.and(filters))
                .build();
    }

    public static Query getTopNQuery() {
        List<DimFilter> filters = new ArrayList<DimFilter>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return new TopNQueryBuilder()
                .threshold(5)
                .metric("agg_count")
                .dataSource("test")
                .intervals(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .granularity(QueryGranularity.NONE)
                .dimension("colo")
                .aggregators(
                        Arrays.<AggregatorFactory>asList(
                                new LongSumAggregatorFactory("agg_count", "agg_count"),
                                new MaxAggregatorFactory("agg_max", "agg_max"),
                                new MinAggregatorFactory("agg_min", "agg_min"),
                                new DoubleSumAggregatorFactory("agg_sum", "agg_sum"),
                                new ApproximateHistogramFoldingAggregatorFactory("agg_histogram", "agg_histogram", 5, 10, null, null)))
                .postAggregators(
                        Arrays.<PostAggregator>asList(new QuantilePostAggregator("agg_quantiles", "agg_histogram", 0.5f)))
                .filters(DimFilters.and(filters)).build();
    }
}
