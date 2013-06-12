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

package com.wibidata.shopping.graph;

import java.util.Map;

import org.kiji.mapreduce.avro.AvroMapReader;
import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.SumNodeMerger;

public class SumKeepAnnotationsNodeMerger extends SumNodeMerger {
  @Override
  protected void update(Node mergedNode, Node newNode) {
    super.update(mergedNode, newNode);

    // Also copy over the annotations.
    if (null != newNode.getAnnotations()) {
      if (null == mergedNode.getAnnotations()) {
        // Copy over all the annotations as-is.
        mergedNode.setAnnotations(newNode.getAnnotations());
      } else {
        // Copy over only the annotations that aren't already set.
        AvroMapReader<String> mergedAnnotations =
            AvroMapReader.<String>create((Map<CharSequence, String>)
                (Map<? extends CharSequence, String>) mergedNode.getAnnotations());
        for (Map.Entry<String, String> annotation
                 : newNode.getAnnotations().entrySet()) {
          if (!mergedAnnotations.containsKey(annotation.getKey())) {
            mergedAnnotations.put(annotation.getKey(), annotation.getValue());
          }
        }
      }
    }
  }
}
