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
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.AvroKVRecordKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import com.wibidata.shopping.avro.TermFrequencies;
import com.wibidata.shopping.avro.TermFrequency;

public class DescriptionWordsTfIdfProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DescriptionWordsTfIdfProducer.class);

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "term_frequencies");
    return builder.build();
  }

  @Override
  public String getOutputColumn() {
    return "info:term_frequencies";
  }

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    AvroKVRecordKeyValueStore<CharSequence, Integer> store =
        new AvroKVRecordKeyValueStore<CharSequence, Integer>();
    AvroKVRecordKeyValueStore.Builder builder = store.builder();
    builder.withInputPath(new Path(getConf().get("doc.frequencies.file")));
    builder.withDistributedCache(true);

    return Collections.<String, KeyValueStore<?, ?>>singletonMap(
        "doc-frequencies", builder.build());
  }

  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    if (!input.containsColumn("info", "term_frequencies")) {
      LOG.info("Product has no term frequencies.");
      return;
    }

    KeyValueStoreReader<CharSequence, Integer> reader = null;
    reader = context.getStore("doc-frequencies");

    if (!reader.isOpen()) {
      throw new IOException("Reader did not get opened properly");
    }

    double totalDocs = reader.get("TotalDocs");

    TermFrequencies freqs =
      input.getMostRecentValue("info", "term_frequencies");

    for (TermFrequency freq : freqs.getFrequencies()) {
      double docFreq = reader.get(freq.getWord());
      double normDocFreq = Math.log(totalDocs / docFreq);

      double tfIdf = freq.getCount() * normDocFreq;
      freq.setTfIdf(tfIdf);
    }

    context.put(freqs);
  }
}
