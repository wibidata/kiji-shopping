#!/usr/bin/env bash

###
# (c) Copyright 2013 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###

rootdir=`dirname $0`
rootdir=`cd ${rootdir} && pwd`

mvn install

export KIJI_INSTANCE=shopping

kiji install --kiji=$KIJI_INSTANCE
kiji-schema-shell --kiji=$KIJI_INSTANCE --file=lib/src/main/layout/kiji_shopping_product.ddl
kiji-schema-shell --kiji=$KIJI_INSTANCE --file=lib/src/main/layout/kiji_shopping_user.ddl
kiji-schema-shell --kiji=$KIJI_INSTANCE --file=lib/src/main/layout/kiji_shopping_category.ddl

user=`whoami`
hadoop fs -put products.json /user/$user/products.json

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji bulk-import \
  --importer=com.wibidata.shopping.bulkimport.ProductBulkImporter \
  --input="format=text file=products.json" \
  --output="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product nsplits=0"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji produce \
  --producer=com.wibidata.shopping.produce.DescriptionWordsTfProducer \
  --input="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product" \
  --output="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product nsplits=0"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji gather \
  --gatherer=com.wibidata.shopping.gather.DescriptionWordsDfGatherer \
  --reducer=org.kiji.mapreduce.lib.reduce.IntSumReducer \
  --input="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product" \
  --output="format=avrokv file=doc_freqs nsplits=1"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji produce \
  -Ddoc.frequencies.file=/user/$user/doc_freqs/part-r-00000.avro \
  --producer=com.wibidata.shopping.produce.DescriptionWordsTfIdfProducer \
  --input="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product" \
  --output="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product nsplits=0"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji gather \
  --gatherer=com.wibidata.shopping.gather.ProductsByCategoryGatherer \
  --reducer=com.wibidata.shopping.reduce.KeepAnnotationsMergeNodeReducer \
  --input="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product" \
  --output="format=avrokv file=products_by_category nsplits=1"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji gather \
  --gatherer=com.wibidata.shopping.gather.ProductsByWordGatherer \
  --reducer=com.wibidata.shopping.reduce.KeepAnnotationsMergeNodeReducer \
  --input="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_product" \
  --output="format=avrokv file=products_by_word nsplits=1"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji bulk-import \
  --importer=com.wibidata.shopping.bulkimport.ProductsByCategoryBulkImporter \
  --input="format=avrokv file=products_by_category/part-r-00000.avro" \
  --output="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_category nsplits=0"

KIJI_CLASSPATH=lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji bulk-import \
  --importer=com.wibidata.shopping.bulkimport.ProductsByCategoryBulkImporter \
  --input="format=avrokv file=products_by_word/part-r-00000.avro" \
  --output="format=kiji table=kiji://.env/$KIJI_INSTANCE/kiji_shopping_category nsplits=0"

KIJI_CLASSPATH=~/.m2/repository/org/kiji/scoring/kiji-scoring/0.2.0/kiji-scoring-0.2.0.jar:lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji fresh \
  --do=register kiji://.env/shopping/kiji_shopping_user/personalization:favorite_words \
  --policy-class=org.kiji.scoring.lib.AlwaysFreshen \
  --producer-class=com.wibidata.shopping.produce.FavoriteFeaturesProducer

KIJI_CLASSPATH=~/.m2/repository/org/kiji/scoring/kiji-scoring/0.2.0/kiji-scoring-0.2.0.jar:lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
kiji fresh \
  --do=register kiji://.env/shopping/kiji_shopping_user/personalization:product_recommendations \
  --policy-class=org.kiji.scoring.lib.AlwaysFreshen \
  --producer-class=com.wibidata.shopping.produce.ProductRecommendationsProducer
