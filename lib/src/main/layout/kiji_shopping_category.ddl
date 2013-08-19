CREATE TABLE kiji_shopping_category
WITH DESCRIPTION 'Categories (topics/items/categories/labels) and their related items'
ROW KEY FORMAT RAW
WITH LOCALITY GROUP default (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  FAMILY related WITH DESCRIPTION 'Category and related products.' (
    product CLASS org.kiji.mapreduce.lib.avro.Node
  )
);
