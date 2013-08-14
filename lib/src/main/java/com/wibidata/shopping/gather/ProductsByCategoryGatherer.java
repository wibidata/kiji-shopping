/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.shopping.gather;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.gather.LabelNodeGatherer;
import org.kiji.mapreduce.lib.graph.EdgeBuilder;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import com.wibidata.shopping.reduce.KeepAnnotationsMergeNodeReducer;

public class ProductsByCategoryGatherer extends LabelNodeGatherer implements Tool {
  public static enum Counters {
    MISSING_FIELDS,
    CATEGORY_EMPTY
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
    drBuilder.newColumnsDef()
        .add("info", "id")
        .add("info", "name")
        .add("info", "description_short")
        .add("info", "inventory")
        .add("info", "price")
        .add("info", "category")
        .add("info", "thumbnail");
    return drBuilder.build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context) throws IOException {
    if (!input.containsColumn("info", "id") || !input.containsColumn("info", "category")
        || !input.containsColumn("info", "description_short")
        || !input.containsColumn("info", "name")
        || !input.containsColumn("info", "thumbnail")) {
      context.incrementCounter(Counters.MISSING_FIELDS);
      return;
    }

    if (input.getMostRecentValue("info", "category").toString().isEmpty()) {
      context.incrementCounter(Counters.CATEGORY_EMPTY);
      return;
    }

    NodeBuilder node = new NodeBuilder()
        .setLabel("category:" + input.getMostRecentValue("info", "category").toString());
    node.addEdge(new EdgeBuilder().setTarget(
        new NodeBuilder().setLabel(input.getMostRecentValue("info", "id").toString())
            .addAnnotation("name", input.getMostRecentValue("info", "name").toString())
            .addAnnotation("description_short", input.getMostRecentValue("info", "description_short").toString())
            .addAnnotation("inventory", input.getMostRecentValue("info", "inventory").toString())
            .addAnnotation("price", input.getMostRecentValue("info", "price").toString())
            .addAnnotation("thumbnail", input.getMostRecentValue("info", "thumbnail").toString())
            .build())
        .build());

    write(node.build(), context);
  }

  @Override
  public int run(String[] args) throws Exception {
    Kiji kiji = Kiji.Factory.open(
        KijiURI.newBuilder().withInstanceName("shopping").build(), getConf());
    KijiTable productTable = kiji.openTable("kiji_shopping_product");
    KijiTable categoryTable = kiji.openTable("kiji_shopping_category");

    try {
      KijiMapReduceJob job = KijiGatherJobBuilder.create()
          .withInputTable(productTable.getURI())
          .withGatherer(ProductsByCategoryGatherer.class)
          .withReducer(KeepAnnotationsMergeNodeReducer.class)
          .withOutput(MapReduceJobOutputs.newAvroKeyMapReduceJobOutput(
              new Path(categoryTable.getName() + "-related-product"), 1))
          .build();
      return job.run() ? 1 : 0;
    } finally {
      ResourceUtils.releaseOrLog(categoryTable);
      ResourceUtils.releaseOrLog(productTable);
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(
        ToolRunner.run(HBaseConfiguration.create(), new ProductsByCategoryGatherer(), args));
  }
}
