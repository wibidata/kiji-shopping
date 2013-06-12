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

import java.util.List;

public class Product {
  private String mId;
  private String mName;
  private String mDescription;
  private String mThumbnail;
  private String mThumbnailXL;
  private List<String> mWords;
  private String mCategory;
  private Long mInventory;
  private Double mPrice;

  public Product() {
  }

  public void setId(String id) {
    mId = id;
  }

  public String getId() {
    return mId;
  }

  public void setName(String name) {
    mName = name;
  }

  public String getName() {
    return mName;
  }

  public void setPrice(Double price) {
    mPrice = price;
  }

  public Double getPrice() {
    return mPrice;
  }

  public void setInventory(Long inventory) {
    mInventory = inventory;
  }

  public Long getInventory() {
    return mInventory;
  }

  public void setDescription(String description) {
    mDescription = description;
  }

  public String getDescription() {
    return mDescription;
  }

  public void setThumbnail(String thumbnail) {
    mThumbnail = thumbnail;
  }

  public String getThumbnail() {
    return mThumbnail;
  }

  public void setThumbnailXL(String thumbnailXL) {
    mThumbnailXL = thumbnailXL;
  }

  public String getThumbnailXL() {
    return mThumbnailXL;
  }

  public void setCategory(String category) {
    mCategory = category;
  }

  public String getCategory() {
    return mCategory;
  }

  public void setWords(List<String> words) {
    mWords = words;
  }

  public List<String> getWords() {
    return mWords;
  }
}
