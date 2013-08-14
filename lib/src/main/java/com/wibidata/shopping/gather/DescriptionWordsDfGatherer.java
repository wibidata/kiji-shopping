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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.lib.reduce.IntSumReducer;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import com.wibidata.shopping.avro.TermFrequencies;
import com.wibidata.shopping.avro.TermFrequency;

public class DescriptionWordsDfGatherer extends KijiGatherer<Text, IntWritable> implements Tool {
  public static enum Counters {
    MISSING_FIELDS
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "term_frequencies");
    return builder.build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context) throws IOException {
    if (!input.containsColumn("info", "term_frequencies")) {
      context.incrementCounter(Counters.MISSING_FIELDS);
      return;
    }

    TermFrequencies freqs = input.getMostRecentValue("info", "term_frequencies");
    IntWritable val = new IntWritable(1);
    for (TermFrequency freq : freqs.getFrequencies()) {
      context.write(new Text(freq.getWord().toString()), val);
    }
    context.write(new Text("TotalDocs"), val);
  }

  @Override
  public int run(String[] args) throws Exception {
    Kiji kiji = Kiji.Factory.open(KijiURI.newBuilder(args[0]).build());
    KijiTable productTable = kiji.openTable("wibi_shopping_product");

    try {
      KijiMapReduceJob job = KijiGatherJobBuilder.create()
          .withInputTable(productTable.getURI())
          .withGatherer(DescriptionWordsDfGatherer.class)
          .withReducer(IntSumReducer.class)
          .withOutput(MapReduceJobOutputs.newAvroKeyValueMapReduceJobOutput(new Path(args[0]), 1))
          .build();
      return job.run() ? 0 : 1;
    } finally {
      ResourceUtils.releaseOrLog(productTable);
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(
        ToolRunner.run(HBaseConfiguration.create(), new DescriptionWordsDfGatherer(), args));
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}
