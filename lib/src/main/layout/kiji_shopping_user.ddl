CREATE TABLE kiji_shopping_user WITH DESCRIPTION 'A table of KijiShopping users (accounts)'
ROW KEY FORMAT HASH PREFIXED(1)
WITH
LOCALITY GROUP default (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Basic information about the user.' (
    login "string",
    password "string",
    name "string"
  ),
  MAP TYPE FAMILY rating CLASS com.wibidata.shopping.avro.ProductRating
),
LOCALITY GROUP short (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  FAMILY personalization WITH DESCRIPTION 'Personalization data about a user.' (
    favorite_words CLASS com.wibidata.shopping.avro.FavoriteWords,
    product_recommendations CLASS com.wibidata.shopping.avro.ProductRecommendations
  )
);
