/*
 * Copyright 2015 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.embedded;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.embedded.helper.IndexCreationHelper;
import io.druid.embedded.helper.QueryCreationHelper;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.segment.QueryableIndex;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;

public class EmbeddedDruidTest {

    @Test
    public void groupByQuery() throws IOException {
        QueryableIndex index = IndexCreationHelper.createDruidSegments();
        Query query = QueryCreationHelper.getGroupByQuery();

        @SuppressWarnings("unchecked")
        Sequence<Row> sequence = QueryHelper.run(query, index);
        ArrayList<Row> results = Sequences.toList(sequence, Lists.<Row>newArrayList());
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

    @Test
    public void topNQuery() throws IOException {
        QueryableIndex index = IndexCreationHelper.createDruidSegments();
        Query query = QueryCreationHelper.getTopNQuery();
        @SuppressWarnings("unchecked")
        Sequence<Result> sequence = QueryHelper.run(query, index);
        ArrayList<Result> results = Sequences.toList(sequence, Lists.<Result>newArrayList());
        Assert.assertEquals(results.size(), 1);
    }

}
