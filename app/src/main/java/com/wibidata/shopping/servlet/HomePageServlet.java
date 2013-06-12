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

package com.wibidata.shopping.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReaderBuilder;

import com.wibidata.shopping.KijiContextListener;
import com.wibidata.shopping.avro.FavoriteWord;
import com.wibidata.shopping.avro.FavoriteWords;
import com.wibidata.shopping.avro.ProductRating;
import com.wibidata.shopping.avro.ProductRecommendation;
import com.wibidata.shopping.avro.ProductRecommendations;
import com.wibidata.shopping.model.Category;
import com.wibidata.shopping.model.Product;

/**
 * The home page servlet.
 */
public class HomePageServlet extends HttpServlet {
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {

    request.setCharacterEncoding("UTF-8");

    final Kiji kiji = (Kiji) getServletContext().getAttribute(KijiContextListener.KIJI_ATTRIBUTE);
    final KijiTable categoryTable = kiji.openTable("kiji_shopping_category");

    // Read the products by category.
    KijiTableReader reader = categoryTable.openTableReader();
    KijiRowScanner scanner = null;
    List<Category> categories = new ArrayList<Category>();
    try {
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef().add("related", "product");
      final KijiDataRequest dataRequest = drBuilder.build();

      final KijiScannerOptions scanOptions = new KijiScannerOptions()
        .setStartRow(categoryTable.getEntityId("category:"))
        .setStopRow(categoryTable.getEntityId("category;"));

      scanner = reader.getScanner(dataRequest, scanOptions);
      for (KijiRowData row : scanner) {
        if (row.containsColumn("related", "product")) {
          Category category = Category.fromProducts((Node) row.getMostRecentValue("related", "product"));
          categories.add(category);
        }
      }
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      ResourceUtils.closeOrLog(scanner);
      ResourceUtils.closeOrLog(reader);
      ResourceUtils.releaseOrLog(categoryTable);
    }
    request.setAttribute("categories", categories);

    // If the user is logged in, read their recent ratings, favorite
    // words, and recommendations.
    if (null != request.getSession().getAttribute("login")) {
      final String login = request.getSession().getAttribute("login").toString();
      final KijiTable userTable = kiji.openTable("kiji_shopping_user");
      final KijiTable productTable = kiji.openTable("kiji_shopping_product");
      try {
        List<ProductRating> ratings = getRecentRatings(userTable, login);
        if (null != ratings) {
          request.setAttribute("recentRatings", ratings);
        }
        List<FavoriteWord> favoriteWords = getFavoriteWords(userTable, login);
        if (null != favoriteWords) {
          request.setAttribute("favoriteWords", favoriteWords);
        }
        List<Product> productRecommendations = getProductRecommendations(userTable, productTable, login);
        if (null != productRecommendations) {
          request.setAttribute("productRecommendations", productRecommendations);
        }
      } finally {
        ResourceUtils.releaseOrLog(productTable);
        ResourceUtils.releaseOrLog(userTable);
      }
    }

    request.getRequestDispatcher("/WEB-INF/view/HomePage.jsp").forward(request, response);
  }

  private List<ProductRating> getRecentRatings(KijiTable userTable, String login) throws IOException {
    KijiTableReader reader = userTable.openTableReader();
    try {
      final long now = System.currentTimeMillis();
      final long lastWeek = now - DateUtils.MILLIS_PER_DAY * 7L;

      EntityId entityId = userTable.getEntityId(login);
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef().addFamily("rating");
      drBuilder.withTimeRange(lastWeek, HConstants.LATEST_TIMESTAMP);
      KijiDataRequest dataRequest = drBuilder.build();

      KijiRowData row = reader.get(entityId, dataRequest);
      if (row.containsColumn("rating")) {
        NavigableMap<String, NavigableMap<Long, ProductRating>> ratingsMap =
            row.getValues("rating");
        NavigableMap<Long, ProductRating> sortedRatings = new TreeMap<Long, ProductRating>();
        for (NavigableMap<Long, ProductRating> value : ratingsMap.values()) {
          for (NavigableMap.Entry<Long, ProductRating> entry : value.entrySet()) {
            sortedRatings.put(entry.getKey(), entry.getValue());
          }
        }
        return new ArrayList<ProductRating>(sortedRatings.descendingMap().values());
      }
      return null;
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reader);
    }
  }

  /**
   * Reads the dish recommendations from the user table.
   *
   * @param userTable The user table.
   * @param productTable The product table.
   * @param login The user's login username.
   * @return A list of the user's product recommendations (may be null).
   */
  private List<Product> getProductRecommendations(KijiTable userTable, KijiTable productTable, String login)
      throws IOException {
    KijiTableReader reader = FreshKijiTableReaderBuilder.create()
        .withTable(userTable)
        .withTimeout(1000).build();
    try {
      EntityId entityId = userTable.getEntityId(login);
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef().add("personalization", "product_recommendations");
      KijiDataRequest dataRequest = drBuilder.build();
      KijiRowData row = reader.get(entityId, dataRequest);
      if (row.containsColumn("personalization", "product_recommendations")) {
        List<Product> productRecommendations = getProducts(productTable,
            row.<ProductRecommendations>getMostRecentValue("personalization", "product_recommendations"));
        return productRecommendations;
      }
      return null;
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reader);
    }
  }

  /**
   * Turns the avro ProductRecommendations type into a POJO for display.
   *
   * @param productRecommendations The avro data.
   * @return A POJO representing the list of dishes.
   */
  private List<Product> getProducts(KijiTable productTable, ProductRecommendations productRecommendations)
      throws IOException {
    List<Product> productList = new ArrayList<Product>();
    List<EntityId> productIds = new ArrayList<EntityId>();
    for (ProductRecommendation productRecommendation : productRecommendations.getRecommendations()) {
      productIds.add(productTable.getEntityId(productRecommendation.getId().toString()));
    }

    // Get the product details for display.
    KijiTableReader reader = productTable.openTableReader();
    try {
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef()
          .add("info", "id")
          .add("info", "name")
          .add("info", "price")
          .add("info", "description_short")
          .add("info", "thumbnail");
      KijiDataRequest dataRequest = drBuilder.build();

      List<KijiRowData> rows = reader.bulkGet(productIds, dataRequest);
      for (KijiRowData row : rows) {
        Product product = new Product();
        product.setId(row.getMostRecentValue("info", "id").toString());
        if (row.containsColumn("info", "name")) {
          product.setName(row.getMostRecentValue("info", "name").toString());
        }
        if (row.containsColumn("info", "description_short")) {
          product.setDescription(row.getMostRecentValue("info", "description_short").toString());
        }
        if (row.containsColumn("info", "thumbnail")) {
          product.setThumbnail(row.getMostRecentValue("info", "thumbnail").toString());
        }
        productList.add(product);
        if (productList.size() >= 4) {
          // Show at most 4 recommendations on the page.
          break;
        }
      }
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reader);
    }

    return productList;
  }

  /**
   * Reads the favorite ingredients from the user table.
   *
   * @param userTable The user table.
   * @param login The user's login username.
   * @return A list of the user's favorite ingredients (may be null).
   */
  private List<FavoriteWord> getFavoriteWords(KijiTable userTable, String login)
      throws IOException {
    KijiTableReader reader = FreshKijiTableReaderBuilder.create()
        .withTable(userTable)
        .withTimeout(1000).build();

    try {
      EntityId entityId = userTable.getEntityId(login);
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef().add("personalization", "favorite_words");
      KijiDataRequest dataRequest = drBuilder.build();

      KijiRowData row = reader.get(entityId, dataRequest);
      if (row.containsColumn("personalization", "favorite_words")) {
        FavoriteWords favoriteWords =
            row.getMostRecentValue("personalization", "favorite_words");
        return favoriteWords.getWords();
      }
      return null;
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reader);
    }
  }
}
