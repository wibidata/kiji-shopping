package com.wibidata.shopping.job

import com.twitter.scalding.Args
import org.kiji.express.flow.{Column, KijiInput, KijiJob}
import org.kiji.mapreduce.lib.graph.NodeBuilder

class ProductPivotJob(args: Args) extends KijiJob(args) {
  KijiInput(args("product-table"))(Map(
      Column("info:id") -> 'id,
      Column("info:name") -> 'name,
      Column("info:description_short") -> 'desc,
      Column("info:price") -> 'price,
      Column("info:category") -> 'category,
      Column("info:thumbnail") -> 'thumb))
    //.map('id, 'name, 'desc, 'price, 'category, 'thumb -> 'nodes) {
      //val NodeBuilder node = new NodeBuilder().setLabel("category:" + )
    //}
}
