package com.wibidata.shopping.bulkimport

import com.twitter.scalding._
import com.wibidata.shopping.avro.DescriptionWords
import org.json4s._
import org.json4s.native.JsonMethods._
import org.kiji.express.EntityId
import org.kiji.express.flow._
import scala.collection.JavaConversions._

class ProductBulkImporterScala(args: Args) extends KijiJob(args) {

  implicit lazy val formats = DefaultFormats

  def parseJson(json: String):
      (String, String, String, String, String, String, String, Double, DescriptionWords) = {
    val product = parse(json)
    ((product \ "Id").extract[String],
        (product \ "Name").extract[String],
        (product \ "DescriptionHtmlComplete").extract[String],
        (product \ "DescriptionHtmlSimple").extract[String].split('\n')(0),
        (product \ "Category").extract[String].capitalize,
        (product \ "Images" \ "PrimaryMedium").extract[String],
        (product \ "Images" \ "PrimaryExtraLarge").extract[String],
        (product \ "ListPrice").extract[Double],
        DescriptionWords.newBuilder.setWords(
          seqAsJavaList((product \ "DescriptionWords").extract[List[String]]
              .map { s => s.toLowerCase })).build())
  }

  TextLine(args("input"))
      .map('line -> ('id, 'name, 'desc, 'desc_short, 'category, 'thumb, 'thumb_xl, 'price, 'desc_words)) { parseJson }
      .map('id -> 'entityId) { id: String => EntityId(id) }
      .write(KijiOutput(args("table-uri"))
          ('name -> "info:name",
           'desc -> "info:description",
           'desc_short -> "info:description_short",
           'category -> "info:category",
           'thumb -> "info:thumbnail",
           'thumb_xl -> "info:thumbnail_xl",
           'price -> "info:price",
           'desc_words -> "info:description_words"))
}
