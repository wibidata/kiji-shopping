CREATE TABLE kiji_shopping_product WITH DESCRIPTION 'A table of products'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Basic information about the product.' (
    id "string",
    name "string",
    description "string",
    description_short "string",
    price "double",
    inventory "long",
    category "string",
    thumbnail "string",
    thumbnail_xl "string",
    term_frequencies CLASS com.wibidata.shopping.avro.TermFrequencies,
    description_words CLASS com.wibidata.shopping.avro.DescriptionWords
  )
);
