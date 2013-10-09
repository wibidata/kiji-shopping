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

package com.wibidata.shopping.model;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wibidata.shopping.avro.ProductInfo;
import com.wibidata.shopping.avro.ProductInfos;
import org.apache.commons.lang.StringUtils;

import org.kiji.mapreduce.avro.AvroMapReader;
import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

public class Category {
  private String mName;
  private List<Product> mProducts;

  public Category() {
    mProducts = new ArrayList<Product>();
  }

  // Used when KijiExpress version of the app is run
  public static Category fromProducts(ProductInfos productInfos) {
    Category category = new Category();
    for (ProductInfo prodInfo : productInfos.getProducts()) {
      if (category.getName() == null) {
        category.setName(prodInfo.getCategory().toString());
      }

      Product product = new Product();
      product.setId(prodInfo.getId().toString());
      product.setName(prodInfo.getName().toString());
      product.setCategory(prodInfo.getCategory().toString());
      product.setDescription(prodInfo.getDescription().toString());
      product.setThumbnail(prodInfo.getThumbnail().toString());
      product.setInventory(0l);
      product.setPrice(prodInfo.getPrice());
      category.addProduct(product);
    }

    return category;
  }

  public static Category fromProducts(Node node) throws MalformedURLException {
    Category category = new Category();
    category.setName(StringUtils.removeStart(node.getLabel().toString(), "category:"));
    for (Edge edge : node.getEdges()) {
      Product product = new Product();
      product.setId(edge.getTarget().getLabel().toString());
      AvroMapReader<String> annotations =
          AvroMapReader.<String>create((Map<CharSequence, String>)
              (Map<? extends CharSequence, String>)
              edge.getTarget().getAnnotations());
      product.setName(annotations.get("name").toString());
      product.setDescription(annotations.get("description_short").toString());
      product.setThumbnail(annotations.get("thumbnail").toString());
      product.setInventory(Long.parseLong(annotations.get("inventory").toString()));
      product.setPrice(Double.parseDouble(annotations.get("price").toString()));
      category.addProduct(product);
    }
    return category;
  }

  public void setName(String name) {
    mName = name;
  }

  public String getName() {
    return mName;
  }

  public List<Product> getProducts() {
    return mProducts;
  }

  public void addProduct(Product product) {
    mProducts.add(product);
  }
}
