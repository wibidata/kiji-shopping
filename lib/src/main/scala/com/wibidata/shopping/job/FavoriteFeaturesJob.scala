package com.wibidata.shopping.job

import com.twitter.scalding.Args
import org.kiji.express.flow._
import org.apache.hadoop.hbase.HConstants
import org.kiji.express.{EntityId, KijiSlice, AvroRecord}

class FavoriteFeaturesJob(args: Args) extends KijiJob(args) {
  final val RECENT_MILLIS: Long = 1000L * 3600L * 24L * 90L

  val wordsByProduct = KijiInput(args("product-table"))(Map(Column("info:term_frequencies") -> 'tfs))
  .map('entityId -> 'productId) { eId: EntityId => eId(0) }
  .map('tfs -> 'tfs) { slice: KijiSlice[AvroRecord] => slice.getFirstValue()("tf_idf_list").asList() }

  KijiInput(args("user-table"),
      Between(System.currentTimeMillis - RECENT_MILLIS, HConstants.LATEST_TIMESTAMP))(
      Map(MapFamily("rating", "") -> 'ratings))
  .flatMapTo(('entityId, 'ratings) -> ('userId, 'productId, 'ratings)) { tuple: (EntityId, KijiSlice[AvroRecord]) =>
    val (eId, slice) = tuple
    for (mapQual <- slice.groupByQualifier) yield (eId(0), mapQual._1, mapQual._2.getFirstValue)
  }
  .filter('ratings) { rating: AvroRecord => rating("value") != 0 }
  .joinWithSmaller('productId -> 'productId, wordsByProduct)
  .flatMapTo(('userId, 'ratings, 'tfs) -> ('userId, 'word, 'weight)) { tuple: (String, AvroRecord, List[AvroRecord]) =>
    val (userId, rating, tfs) = tuple
    for (tf <- tfs) yield (userId, tf("word").asString(), rating("value").asInt() * tf("tf_idf").asDouble())
  }
  .groupBy('userId, 'word) { _.sum { 'weight } }
  .groupBy('userId) { _.sortedReverseTake[(Double, String, String)] (('weight, 'userId, 'word) -> 'top, 10) }
  .flattenTo[(Double, String, String)]('top -> ('weight, 'userId, 'word))
  .packAvro(('word, 'weight) -> 'word)
  .groupBy('userId) { _.toList[AvroRecord]('word -> 'words) }
  .packAvro(('words) -> 'favorite_words)
  .map('userId -> 'entityId) { userId: String => EntityId(userId) }
  .write(KijiOutput(args("user-table")('favorite_words -> "personalization:favorite_words")))
}
