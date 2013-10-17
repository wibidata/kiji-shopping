package com.wibidata.shopping.job

import com.twitter.scalding.{TypedPipe, Tsv, Args}
import org.kiji.express.flow._
import org.apache.hadoop.hbase.HConstants
import org.kiji.express.{EntityId, KijiSlice, AvroRecord}
import org.kiji.schema.{Kiji, KijiTableReader}

class FavoriteFeaturesJobScorer(args: Args) extends KijiJob(args) {
  final val RECENT_MILLIS: Long = 1000L * 3600L * 24L * 90L

  val wordsByProduct = KijiInput(args("product-table"))(Map(Column("info:term_frequencies") -> 'tfs))
  .mapTo(('entityId, 'tfs) -> ('productId, 'tfs)) { tuple: (EntityId, KijiSlice[AvroRecord]) =>
    val (eId, slice) = tuple
    (eId(0), slice.getFirstValue()("tf_idf_list").asList())
  }

  val wordsByProductMap = Map("foo" -> List(AvroRecord()))

  KijiInput(args("user-table"),
      Between(System.currentTimeMillis - RECENT_MILLIS, HConstants.LATEST_TIMESTAMP))(
      Map(MapFamily("rating", "") -> 'ratings))
  .mapTo(('entityId, 'ratings) -> ('entityId, 'words)) { tuple: (EntityId, KijiSlice[AvroRecord]) =>
    // THIS IS THE PRODUCER
    val (eId, slice) = tuple

    val favoriteWords = slice.groupByQualifier
        // Scala map can only ever take one element, while a Scalding map may take as many as necessary
        .map { case (prodId, ratingSlice) => (prodId, ratingSlice.getFirstValue) }
        .filter { case (prodId, rating) => rating("value").asInt() != 0 }
        .map { case (prodId, rating) => (prodId, rating, wordsByProductMap.get(prodId)) }
        .flatMap { case (prodId, rating, words) => for (word <- words) yield
            (prodId, word("word").asString(), rating("value").asInt() * word("tf_idf").asDouble())
        }.groupBy { case (prodId, word, weight) => (prodId, word) }
        .map { case ((prodId, word), groupMembers) =>
            (word, groupMembers.foldLeft(0) { case (total, (x, y, weight)) => total + weight })
        }.toList
        .sortBy { case (word, weight) => -weight }
        .take(10)

    (eId, favoriteWords)
  }
}
