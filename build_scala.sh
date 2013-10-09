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

express job lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.wibidata.shopping.bulkimport.ProductBulkImporterScala \
  --table-uri kiji://.env/$KIJI_INSTANCE/kiji_shopping_product \
  --input /user/natty/products.json \
  --hdfs

express job lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.wibidata.shopping.job.TfIdfJob \
  --product-table kiji://.env/shopping/kiji_shopping_product \
  --hdfs

express job lib/target/lib-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.wibidata.shopping.job.ProductPivotJob \
  --product-table kiji://.env/shopping/kiji_shopping_product \
  --category-table kiji://.env/shopping/kiji_shopping_category \
  --hdfs
