package com.wibidata.shopping.job

import org.kiji.express.flow._
import com.twitter.scalding.Args
import org.kiji.express._
import org.kiji.schema.KijiURI

class TfIdfJob(args: Args) extends KijiJob(args) {

  val inputs = KijiInput(args("product-table"))(
      Map(Column("info:name") -> 'prod_name, Column("info:description_words") -> 'words))
      .map('entityId -> 'entityId) {
        eId: EntityId => eId.toJavaEntityId(
            KijiURI.newBuilder(args("product-table")).build()).getComponentByIndex(0).toString
      }
      .map('words -> 'words) {
        slice: KijiSlice[AvroRecord] => slice.getFirstValue()("words").asList()
      }
      .flatMap('words -> 'word) { words: List[AvroString] => words.map { word => word.asString() } }
      .project('entityId, 'word)

  val wordCountInDoc = inputs
      .groupBy('entityId, 'word) { _.size }
      .rename('word -> 'tf_word)
      .rename('size -> 'tf_count)
      .project('entityId, 'tf_word, 'tf_count)

  val totalWordCountPerDoc = wordCountInDoc
      .groupBy('entityId) { _.sum { 'tf_count -> 'total_count } }
      .project('entityId, 'total_count)

  val tf = wordCountInDoc
      .joinWithSmaller('entityId -> 'entityId, totalWordCountPerDoc)
      .map(('tf_count, 'total_count) -> 'tf_count) { x: (Double, Double) => x._1 / x._2 }
      .project('entityId, 'tf_word, 'tf_count)

  val d = inputs
      .unique('entityId)
      .groupAll { _.size }
      .rename('size -> 'n_docs)

  val df = inputs
      .groupBy('word) { _.size }
      .rename('word -> 'df_word)
      .rename('size -> 'df_count)
      .project('df_word, 'df_count)

  val idf = df.crossWithTiny(d)

  def log2(x: Double) = math.log10(x) / math.log10(2)

  val tf_idf = tf
      .joinWithSmaller('tf_word -> 'df_word, idf)
      .map(('tf_count, 'n_docs, 'df_count) -> 'tf_idf) {
        x : (Double, Double, Double) =>
        val (tfCount, nDocs, dfCount) = x

        val idf  = log2(nDocs / (dfCount))

        (tfCount * idf)
      }
      .project('entityId, 'tf_idf, 'tf_word)
      .map(('tf_word, 'tf_idf) -> 'avro_tf_idf) {
        x: (String, Double) =>
            AvroRecord("word" -> x._1, "count" -> 0, "tf_idf" -> x._2)
      }
      .groupBy('entityId) { _.toList[AvroRecord]('avro_tf_idf -> 'tf_idf_list) }
      .packAvro('tf_idf_list -> 'frequencies)
      .map('entityId -> 'entityId) { eId: String => EntityId(eId) }
      .write(KijiOutput(args("product-table"))('frequencies -> "info:term_frequencies"))
}
