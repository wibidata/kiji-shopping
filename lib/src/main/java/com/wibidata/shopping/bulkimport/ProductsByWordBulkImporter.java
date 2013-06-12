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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.lib.avro.Node;

public class ProductsByWordBulkImporter extends AvroKVBulkImporter<CharSequence, Node> {

  @Override
  public void produce(AvroKey<CharSequence> key, AvroValue<Node> value, KijiTableContext context)
      throws IOException {
    context.put(context.getEntityId(key.datum().toString()), "related", "product", value.datum());
  }

  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return Schema.create(Type.STRING);
  }

  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return Node.SCHEMA$;
  }
}
