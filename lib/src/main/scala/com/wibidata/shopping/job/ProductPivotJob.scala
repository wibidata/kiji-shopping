package com.wibidata.shopping.job

import org.kiji.express.flow._
import org.kiji.express._
import com.twitter.scalding.Args

class ProductPivotJob(args: Args) extends KijiJob(args) {

  val inputs = KijiInput(args("product-table"))(Map(
      Column("info:name") -> 'name,
      Column("info:description_short") -> 'desc,
      Column("info:price") -> 'price,
      Column("info:category") -> 'category,
      Column("info:thumbnail") -> 'thumb,
      Column("info:term_frequencies") -> 'tf_idf))
    .mapTo(('entityId, 'name, 'desc, 'price, 'category, 'thumb, 'tf_idf) -> ('id, 'name, 'desc, 'price, 'category, 'thumb, 'tf_idf)) {
    cols: (EntityId, KijiSlice[String], KijiSlice[String], KijiSlice[Double], KijiSlice[String], KijiSlice[String], KijiSlice[AvroRecord]) =>
      val (entityId, name, desc, price, category, thumb, tf_idf) = cols
      (entityId(0), name.getFirstValue(), desc.getFirstValue(), price.getFirstValue(),
          category.getFirstValue(), thumb.getFirstValue(), tf_idf.getFirstValue()("tf_idf_list" /*should change to frequencies*/).asList())
  }

  val categoryPivot = inputs.mapTo(('id, 'name, 'desc, 'price, 'category, 'thumb) -> ('category, 'product)) {
    cols: (String, String, String, Double, String, String) =>
      val (id, name, desc, price, category, thumb) = cols
      ("category:" + category, AvroRecord(
        "category" -> category,
        "id" -> id,
        "name" -> name,
        "description" -> desc,
        "price" -> price,
        "thumbnail" -> thumb))
  }
  .groupBy('category) { _.toList[AvroRecord]('product -> 'products) }
  .packAvro(('products) -> 'products)
  .map('category -> 'entityId) { category: String => EntityId(category) }
  .project('entityId, 'products)

  val termPivot = inputs.flatMapTo(('id, 'name, 'desc, 'price, 'category, 'thumb, 'tf_idf) -> ('word, 'product)) {
    cols: (String, String, String, Double, String, String, List[AvroRecord]) =>
      val (id, name, desc, price, category, thumb, tf_idf) = cols
      for (tf <- tf_idf) yield
        ("word:" + tf("word").asString(), AvroRecord(
          "termWeight" -> tf("tf_idf").asDouble(),
          "category" -> category,
          "id" -> id,
          "name" -> name,
          "description" -> desc,
          "price" -> price,
          "thumbnail" -> thumb))
  }
  .groupBy('word) { _.toList[AvroRecord]('product -> 'products) }
  .packAvro(('products) -> 'products)
  .map('word -> 'entityId) { word: String => EntityId(word) }
  .project('entityId, 'products)

  (categoryPivot ++ termPivot).write(KijiOutput(args("category-table"))('products -> "related:exp_product"))
}
