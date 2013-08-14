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
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.kiji.mapreduce.lib.graph.EdgeBuilder;
import org.kiji.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.mapreduce.lib.produce.SumPathNaiveRecommendationProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.tools.ToolUtils;

import com.wibidata.shopping.avro.FavoriteWord;
import com.wibidata.shopping.avro.FavoriteWords;
import com.wibidata.shopping.avro.ProductRecommendation;
import com.wibidata.shopping.avro.ProductRecommendations;

public class ProductRecommendationsProducer extends SumPathNaiveRecommendationProducer {
  private static final Logger LOG = LoggerFactory.getLogger(ProductRecommendationsProducer.class);

  private KijiRowData mRowData;

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("personalization", "favorite_words")
                           .addFamily("rating");
    return builder.build();
  }

  @Override
  public String getOutputColumn() {
    return "personalization:product_recommendations";
  }

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    KijiTableKeyValueStore<Node> store = KijiTableKeyValueStore.builder()
        .withTable(KijiURI.newBuilder()
            .withInstanceName("shopping")
            .withTableName("kiji_shopping_category").build())
        .withColumn("related", "product")
        .build();
    store.setConf(HBaseConfiguration.addHbaseResources(getConf()));

    return Collections.<String, KeyValueStore<?, ?>>singletonMap(ASSOCIATIONS_STORE, store);
  }

  @Override
  protected Node getAffinities(KijiRowData input, ProducerContext context)
      throws IOException {
    mRowData = input;

    NodeBuilder node = new NodeBuilder().setLabel("me");
    if (!input.containsColumn("personalization", "favorite_words")) {
      return node.build();
    }

    FavoriteWords favoriteWords =
        input.getMostRecentValue("personalization", "favorite_words");
    for (FavoriteWord word : favoriteWords.getWords()) {
      node.addEdge(new EdgeBuilder()
          .setWeight(word.getWeight())
          .setTarget(new NodeBuilder().setLabel("word:" + word.getWord()).build())
          .build());
    }
    return node.build();
  }

  /**
   * Looks up an entity's related items in the association model via the KeyValue store.
   *
   * @param entityLabel The entity id to look up associations (related items) for.
   * @param context The producer context.
   * @return The entity's node in the relationship graph. Its outgoing edges represent its
   *     direct relationships.  This will not return null; if there are no related items,
   *     it will return a node with no outgoing edges.
   * @throws IOException If there is an error.
   */
  @Override
  protected Node getRelatedItems(String entityLabel, ProducerContext context)
      throws IOException {
    KeyValueStoreReader<KijiRowKeyComponents, Node> associationsStore = context.getStore(ASSOCIATIONS_STORE);
    assert null != associationsStore;

    KijiRowKeyComponents eid = KijiRowKeyComponents.fromComponents(entityLabel);

    Node associations = associationsStore.get(eid);
    if (null == associations) {
      // No associations, just return an empty node.
      return new NodeBuilder().setLabel(entityLabel).build();
    }
    return associations;
  }

  @Override
  public void setup(KijiContext context) throws IOException {
    KeyValueStoreReader<EntityId, Node> associationsStore = context.getStore(ASSOCIATIONS_STORE);
  }

  @Override
  public void write(Node recommendations, ProducerContext context)
      throws IOException {
    ProductRecommendations productRecommendations = new ProductRecommendations();
    productRecommendations.setRecommendations(new ArrayList<ProductRecommendation>());

    for (Edge edge : recommendations.getEdges()) {
      if (hasRating(edge.getTarget().getLabel().toString())) {
        // If the user has already rated it, we don't want to include it.
        continue;
      }
      ProductRecommendation prodRecommendation = new ProductRecommendation();
      prodRecommendation.setId(edge.getTarget().getLabel());
      prodRecommendation.setWeight(edge.getWeight());
      productRecommendations.getRecommendations().add(prodRecommendation);
    }
    context.put(productRecommendations);
  }

  private boolean hasRating(String productId) {
    return mRowData.containsColumn("rating", productId);
  }
}
