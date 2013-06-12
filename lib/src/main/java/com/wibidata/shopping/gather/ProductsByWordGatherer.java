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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.lib.gather.LabelNodeGatherer;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import com.wibidata.shopping.avro.TermFrequencies;
import com.wibidata.shopping.avro.TermFrequency;

public class ProductsByWordGatherer extends LabelNodeGatherer {

  private static final Logger LOG = LoggerFactory.getLogger(ProductsByWordGatherer.class);

  public static enum Counters {
    MISSING_FIELDS
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "id")
                           .add("info", "name")
                           .add("info", "description")
                           .add("info", "price")
                           .add("info", "inventory")
                           .add("info", "category")
                           .add("info", "thumbnail")
                           .add("info", "term_frequencies");
    return builder.build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context) throws IOException {
    if (!input.containsColumn("info", "id") || !input.containsColumn("info", "term_frequencies")
        || !input.containsColumn("info", "description") || !input.containsColumn("info", "name")
        || !input.containsColumn("info", "thumbnail")) {
      context.incrementCounter(Counters.MISSING_FIELDS);
      return;
    }

    TermFrequencies words = input.getMostRecentValue("info", "term_frequencies");
    for (TermFrequency word : words.getFrequencies()) {
      NodeBuilder node = new NodeBuilder("word:" + word.getWord());
      NodeBuilder target = node.addEdge()
        .setWeight(word.getTfIdf())
        .target(input.getMostRecentValue("info", "id").toString())
          .addAnnotation("name", input.getMostRecentValue("info", "name").toString())
          .addAnnotation("description", input.getMostRecentValue("info", "description").toString())
          .addAnnotation("price", input.getMostRecentValue("info", "price").toString())
          .addAnnotation("inventory", input.getMostRecentValue("info", "inventory").toString())
          .addAnnotation("thumbnail", input.getMostRecentValue("info", "thumbnail").toString());
      if (input.containsColumn("info", "category")) {
        target.addAnnotation("category", input.getMostRecentValue("info", "category").toString());
      }
      write(node.build(), context);
    }
  }
}
