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

package com.wibidata.shopping.produce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput.RowOptions;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;

import com.wibidata.shopping.avro.DescriptionWords;
import com.wibidata.shopping.avro.TermFrequencies;
import com.wibidata.shopping.avro.TermFrequency;

public class DescriptionWordsTfProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DescriptionWordsTfProducer.class);

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "description_words");
    return builder.build();
  }

  @Override
  public String getOutputColumn() {
    return "info:term_frequencies";
  }

  @Override
  public void produce(KijiRowData input, ProducerContext context) throws IOException {
    if (!input.containsColumn("info", "description_words")) {
      LOG.info("Product has no description words.");
      return;
    }

    Map<CharSequence, TermFrequency> frequencies = new HashMap<CharSequence, TermFrequency>();
    DescriptionWords words =
        input.getMostRecentValue("info", "description_words");

    for (CharSequence word : words.getWords()) {
      if (frequencies.containsKey(word)) {
        TermFrequency wordFreq = frequencies.get(word);
        wordFreq.setCount(wordFreq.getCount() + 1);
      } else {
        TermFrequency wordFreq = TermFrequency.newBuilder()
          .setWord(word)
          .setCount(1)
          .build();
        frequencies.put(word, wordFreq);
      }
    }

    List<TermFrequency> freqList = new ArrayList<TermFrequency>(frequencies.values());
    TermFrequencies allFrequencies = TermFrequencies.newBuilder()
      .setFrequencies(freqList)
      .build();
    context.put(allFrequencies);
  }

  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    KijiURI productTable = KijiURI.newBuilder()
        .withInstanceName("shopping")
        .withTableName("kiji_shopping_product")
        .build();

    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "description_words");
    KijiDataRequest dataReq = builder.build();

    KijiMapReduceJob job = null;
    if (args[0] == "tbl") {
      job = KijiProduceJobBuilder.create()
          .withConf(HBaseConfiguration.create())
          .withInputTable(productTable)
          .withProducer(DescriptionWordsTfProducer.class)
          .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(productTable))
          .build();
    } else {
      job = KijiProduceJobBuilder.create()
          .withConf(HBaseConfiguration.create())
          .withJobInput(MapReduceJobInputs.newKijiTableMapReduceJobInput(
              productTable, dataReq, RowOptions.create()))
          .withProducer(DescriptionWordsTfProducer.class)
          .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(productTable, 0))
          .build();
    }

    job.run();
  }
}
