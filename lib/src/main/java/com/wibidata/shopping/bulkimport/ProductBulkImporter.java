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

package com.wibidata.shopping.bulkimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

import com.wibidata.shopping.avro.DescriptionWords;

public class ProductBulkImporter extends KijiBulkImporter<LongWritable, Text> {

  /**
   * Kiji will call this method once for each line in the input text file.
   * Each line in the input text file is a JSON object that has fields describing the product.
   *
   * This method should parse the JSON text line to extract relevant
   * information about the product. These facts about the product should be
   * written to the columns of a row in the kiji_shopping_product table.
   *
   * @param line The input text line of JSON.
   * @param context A helper object used to write to the KijiTable.
   * @throws IOException
   */
  @Override
  public void produce(LongWritable offset, Text line, KijiTableContext context)
      throws IOException {
    final JsonNode json = parseJson(line.toString());

    // Parse the ID of the product.
    if (null == json.get("Id")) {
      return;
    }
    final String productId = json.get("Id").getTextValue();

    // Use the ID of the dish as the ID of the WibiTable row.
    final EntityId entityId = context.getEntityId(productId);

    context.put(entityId, "info", "id", productId);

    if (null == json.get("Name")) {
      return;
    }
    context.put(entityId, "info", "name", json.get("Name").getTextValue());

    if (null != json.get("DescriptionHtmlComplete")) {
      context.put(entityId, "info", "description",
          json.get("DescriptionHtmlComplete").getTextValue());
    }

    if (null != json.get("DescriptionHtmlSimple")) {
      String simpleDesc = json.get("DescriptionHtmlSimple").getTextValue();
      String shortDesc = StringUtils.split(simpleDesc, '\n')[0];
      context.put(entityId, "info", "description_short", shortDesc);
    }

    if (null != json.get("Category")) {
      String category = json.get("Category").getTextValue().toLowerCase();
      context.put(entityId, "info", "category", StringUtils.capitalize(category));
    }

    if (null != json.get("Images").get("PrimaryMedium")) {
      context.put(entityId, "info", "thumbnail",
          json.get("Images").get("PrimaryMedium").getTextValue());
    }

    if (null != json.get("Images").get("PrimaryExtraLarge")) {
      context.put(entityId, "info", "thumbnail_xl",
          json.get("Images").get("PrimaryExtraLarge").getTextValue());
    }

    if (null != json.get("ListPrice")) {
      context.put(entityId, "info", "price", json.get("ListPrice").getDoubleValue());
    }

    if (null != json.get("Skus").get(0).get("QuantityAvailable")) {
      context.put(entityId, "info", "inventory",
          json.get("Skus").get(0).get("QuantityAvailable").getLongValue());
    }

    List<CharSequence> words = new ArrayList<CharSequence>();
    for (JsonNode word : json.get("DescriptionWords")) {
      words.add(word.getTextValue().toLowerCase());
    }
    DescriptionWords prodWords = DescriptionWords.newBuilder().setWords(words).build();
    context.put(entityId, "info", "description_words", prodWords);
  }

  /**
   * Parses the JSON line describing a product.
   *
   * @param jsonInput The JSON string.
   * @return The parsed JSON object.
   */
  private JsonNode parseJson(String jsonInput) throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(jsonInput, JsonNode.class);
  }
}
