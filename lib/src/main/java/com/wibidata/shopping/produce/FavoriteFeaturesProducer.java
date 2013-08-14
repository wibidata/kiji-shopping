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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.kiji.mapreduce.lib.graph.EdgeBuilder;
import org.kiji.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.mapreduce.lib.graph.NodeUtils;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;

import com.wibidata.shopping.avro.DescriptionWords;
import com.wibidata.shopping.avro.FavoriteWord;
import com.wibidata.shopping.avro.FavoriteWords;
import com.wibidata.shopping.avro.ProductRating;
import com.wibidata.shopping.avro.TermFrequencies;
import com.wibidata.shopping.avro.TermFrequency;

public class FavoriteFeaturesProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(FavoriteFeaturesProducer.class);
  private static final long RECENT_MILLIS = 1000L * 3600L * 24L * 90L;

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("rating");
    builder.withTimeRange(System.currentTimeMillis() - RECENT_MILLIS, HConstants.LATEST_TIMESTAMP);
    return builder.build();
  }

  @Override
  public String getOutputColumn() {
    return "personalization:favorite_words";
  }

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    KijiTableKeyValueStore<DescriptionWords> store =
        KijiTableKeyValueStore.builder()
            .withTable(KijiURI.newBuilder()
                .withInstanceName("shopping")
                .withTableName("kiji_shopping_product").build())
            .withColumn("info", "term_frequencies")
            .build();
    store.setConf(HBaseConfiguration.addHbaseResources(getConf()));

    return Collections.<String, KeyValueStore<?, ?>>singletonMap(
        "words-by-product", store);
  }

  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    if (!input.containsColumn("rating")) {
      LOG.info("User has no ratings.");
      return;
    }

    List<Node> wordNodes = new ArrayList<Node>();

    NavigableMap<String, ProductRating> productRatings =
        input.getMostRecentValues("rating");
    LOG.info("Found " + productRatings.size() + " ratings.");
    for (Map.Entry<String, ProductRating> ratingEntry : productRatings.entrySet()) {
      String productId = ratingEntry.getKey();
      ProductRating productRating = ratingEntry.getValue();
      if (productRating.getValue() != 0) {
        LOG.info("Found a rating with a non-zero value.");
        List<TermFrequency> words = null;
        try {
          words = getWords(productId, context);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        LOG.info("Adding " + words.size() + " words.");
        for (TermFrequency word : words) {
          if (word.getTfIdf() < 2) {
            continue;
          }
          NodeBuilder node = new NodeBuilder()
              .setLabel("all")
              .addEdge(new EdgeBuilder()
                  .setWeight(productRating.getValue().intValue() * word.getTfIdf())
                  .build());
          wordNodes.add(node.build());
        }
      }
    }

    if (wordNodes.isEmpty()) {
      LOG.info("No words.");
      return;
    }

    FavoriteWords favoriteWords = new FavoriteWords();
    favoriteWords.setWords(new ArrayList<FavoriteWord>());
    TreeSet<ComparableFavoriteWord> sortedWords = new TreeSet<ComparableFavoriteWord>();

    List<Node> mergedWords = NodeUtils.mergeNodes(wordNodes);

    LOG.info("A total of " + wordNodes.size() + " words were merged down to "
        + mergedWords.get(0).getEdges().size());
    for (Node node : mergedWords) {
      for (Edge edge : node.getEdges()) {
        sortedWords.add(new ComparableFavoriteWord(
            edge.getTarget().getLabel(), edge.getWeight()));
      }
    }

    for (ComparableFavoriteWord compFavWord : sortedWords.descendingSet()) {
      if (favoriteWords.getWords().size() >= 10) break;
      FavoriteWord favoriteWord = FavoriteWord.newBuilder()
          .setWord(compFavWord.getWord())
          .setWeight(compFavWord.getWeight())
          .build();
      favoriteWords.getWords().add(favoriteWord);
    }

    if (!favoriteWords.getWords().isEmpty()) {
      LOG.info("Writing favorite words.");
      context.put(favoriteWords);
    }
  }

  private List<TermFrequency> getWords(String productId, ProducerContext context)
      throws IOException, InterruptedException {
    LOG.info("Looking up words for product " + productId);
    KeyValueStoreReader<KijiRowKeyComponents, TermFrequencies> kvsReader =
        context.<KijiRowKeyComponents, TermFrequencies>getStore("words-by-product");
    TermFrequencies words = kvsReader.get(KijiRowKeyComponents.fromComponents(productId));

    if (null == words) {
      return Collections.<TermFrequency>emptyList();
    }
    LOG.info("Found " + words.getFrequencies().size() + " words.");
    return words.getFrequencies();
  }
}
