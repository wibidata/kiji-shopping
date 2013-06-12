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

public class ComparableFavoriteWord
    implements Comparable<ComparableFavoriteWord> {
  private CharSequence word;
  private Double weight;

  public ComparableFavoriteWord(CharSequence word, Double weight) {
    this.word = word;
    this.weight = weight;
  }

  public CharSequence getWord() {
    return this.word;
  }

  public Double getWeight() {
    return this.weight;
  }

  @Override
  public int compareTo(ComparableFavoriteWord o) {
    return weight.compareTo(o.weight);
  }
}
