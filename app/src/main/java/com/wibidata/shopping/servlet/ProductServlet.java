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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.util.ResourceUtils;

import com.wibidata.shopping.KijiContextListener;
import com.wibidata.shopping.avro.DescriptionWords;
import com.wibidata.shopping.avro.ProductRating;
import com.wibidata.shopping.model.Product;

/**
 * The dish page servlet (where we show dish details).
 */
public class ProductServlet extends HttpServlet {
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {

    request.setCharacterEncoding("UTF-8");

    final String productId = request.getParameter("id");

    final Kiji kiji = (Kiji) getServletContext().getAttribute(KijiContextListener.KIJI_ATTRIBUTE);
    final KijiTable productTable = kiji.openTable("kiji_shopping_product");

    final KijiTableReader reader = productTable.openTableReader();
    try {
      final KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef()
          .add("info", "name")
          .add("info", "description")
          .add("info", "thumbnail")
          .add("info", "thumbnail_xl")
          .add("info", "price")
          .add("info", "inventory")
          .add("info", "description_words")
          .add("info", "category");
      final KijiDataRequest dataRequest = drBuilder.build();

       final KijiRowData row = reader.get(productTable.getEntityId(productId), dataRequest);
       if (row.containsColumn("info", "name")) {
         Product product = new Product();
         product.setId(productId);
         product.setName(row.getMostRecentValue("info", "name").toString());
         if (row.containsColumn("info", "description")) {
           product.setDescription(row.getMostRecentValue("info", "description").toString());
         }
         if (row.containsColumn("info", "thumbnail")) {
           product.setThumbnail(row.getMostRecentValue("info", "thumbnail").toString());
         }
         if (row.containsColumn("info", "thumbnail_xl")) {
           product.setThumbnailXL(row.getMostRecentValue("info", "thumbnail_xl").toString());
         }
         if (row.containsColumn("info", "price")) {
           product.setPrice(row.<Double>getMostRecentValue("info", "price"));
         }
         if (row.containsColumn("info", "inventory")) {
           product.setInventory(row.<Long>getMostRecentValue("info", "inventory"));
         }
         if (row.containsColumn("info", "description_words")) {
           product.setWords(
               getWords(row.<DescriptionWords>getMostRecentValue("info", "description_words")));
         }
         if (row.containsColumn("info", "category")) {
           product.setCategory(row.getMostRecentValue("info", "category").toString());
         }
         request.setAttribute("product", product);
       }
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
       IOUtils.closeQuietly(reader);
       ResourceUtils.releaseOrLog(productTable);
    }

    // Rate the item if necessary.
    final String rating = request.getParameter("rate");
    final boolean isLoggedIn = null != request.getSession().getAttribute("login");
    if (isLoggedIn) {
      final String login = request.getSession().getAttribute("login").toString();
      final KijiTable userTable = kiji.openTable("kiji_shopping_user");
      try {
        if (null != rating) {
          // Write the user's rating to the user table.
          final KijiTableWriter writer = userTable.openTableWriter();
          try {
            final ProductRating productRating = ProductRating.newBuilder()
                .setProductId(productId)
                .setProductName(((Product) request.getAttribute("product")).getName())
                .setValue(Integer.parseInt(rating))
                .build();
            writer.put(userTable.getEntityId(login), "rating", productId, productRating);
          } finally {
            IOUtils.closeQuietly(writer);
          }
          request.setAttribute("currentRating", rating);
        } else {
          // Read the user's existing rating for this dish (if any).
          final KijiTableReader userTableReader = userTable.openTableReader();
          try {
            KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
            drBuilder.newColumnsDef().add("rating", productId);
            KijiDataRequest ratingRequest = drBuilder.build();
            KijiRowData ratingResult =
                userTableReader.get(userTable.getEntityId(login), ratingRequest);
            if (ratingResult.containsColumn("rating", productId)) {
              int currentRating =
                  ratingResult.<ProductRating>getMostRecentValue("rating", productId).getValue();
              request.setAttribute("currentRating", currentRating);
            }
          } catch (KijiDataRequestException e) {
            throw new IOException(e);
          } finally {
            IOUtils.closeQuietly(userTableReader);
          }
        }
      } finally {
        ResourceUtils.releaseOrLog(userTable);
      }
    }

    request.getRequestDispatcher("/WEB-INF/view/Product.jsp").forward(request, response);
  }

  /**
   * Parses an Avro DescriptionWords record into a regular list of strings so
   * it can be safely rendered in a JSP page.
   *
   * @param words The description_words avro record from the Wibi table.
   * @return A list of strings.
   */
  private List<String> getWords(DescriptionWords words) {
    List<String> wordList = new ArrayList<String>();
    if (null != words.getWords()) {
      for (CharSequence word : words.getWords()) {
        wordList.add(word.toString());
      }
    }
    return wordList;
  }
}
