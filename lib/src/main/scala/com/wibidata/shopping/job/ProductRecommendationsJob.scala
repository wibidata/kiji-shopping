package com.wibidata.shopping.job

import com.twitter.scalding.Args
import org.kiji.express.flow._
import org.apache.hadoop.hbase.HConstants
import org.kiji.express.{EntityId, KijiSlice, AvroRecord}

class ProductRecommendationsJob(args: Args) extends KijiJob(args) {
  final val RECENT_MILLIS: Long = 1000L * 3600L * 24L * 90L

  val wordsByProduct = KijiInput(args("category-table"))(Map(Column("related:exp_product") -> 'related))
  .map('entityId -> 'word) { eId: EntityId => eId(0) }
  .map('related -> 'related) { slice: KijiSlice[AvroRecord] => slice.getFirstValue()("products").asList() }

  /*KijiInput(args("user-table"))(Map(MapFamily("rating", "") -> 'ratings),
                                    Column("personalization:favorite_words") -> 'words)
  .flatMapTo(('entityId, 'ratings, 'words) -> ('userId, 'productId, 'words)) {
    tuple: (EntityId, KijiSlice[AvroRecord], KijiSlice[AvroRecord]) =>
      val (eId, prodSlice, wordSlice) = tuple

      for (mapQual <- slice.groupByQualifier) yield (eId(0), mapQual._1, mapQual._2.getFirstValue)
  }*/
}
