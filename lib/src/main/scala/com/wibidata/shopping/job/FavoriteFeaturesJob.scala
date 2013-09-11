package com.wibidata.shopping.job

import com.twitter.scalding.Args
import org.kiji.express.flow._
import org.apache.hadoop.hbase.HConstants
import org.kiji.express.{KijiSlice, AvroRecord}

class FavoriteFeaturesJob(args: Args) extends KijiJob(args) {
  final val RECENT_MILLIS: Long = 1000L * 3600L * 24L * 90L

  KijiInput(args("user-table"),
      Between(System.currentTimeMillis - RECENT_MILLIS, HConstants.LATEST_TIMESTAMP))(
      Map(Column("rating") -> 'ratings))
    //.map('ratings -> 'related_terms) { slices: KijiSlice[AvroRecord] =>

    //}
}
