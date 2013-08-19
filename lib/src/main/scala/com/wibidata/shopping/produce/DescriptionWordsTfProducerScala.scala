package com.wibidata.shopping.produce

import org.kiji.express.flow._
import com.twitter.scalding.{Tsv, Args}
import org.kiji.express._

class DescriptionWordsTfProducerScala(args: Args) extends KijiJob(args) {

  val inputs = KijiInput(args("product-table"))(
      Map(Column("info:name") -> 'prod_name, Column("info:description_words") -> 'words))
      .map('entityId -> 'entityIdStr) { eId: EntityId => eId.toString }
      .map('words -> 'words) {
        slice: KijiSlice[AvroRecord] => slice.getFirstValue()("words").asList()
      }
      .flatMap('words -> 'word) { words: List[AvroString] => words.map { word => word.asString() } }
      .project('entityId, 'entityIdStr, 'word)

  val wordCountInDoc = inputs
      .groupBy('entityIdStr, 'word) { group => group.size }
      .project('entityId, 'entityIdStr, 'word, 'size)
      .rename('word -> 'tf_word)
      .project('entityId, 'entityIdStr, 'tf_word, 'size)
      .rename('size -> 'tf_count)
      .project('entityId, 'entityIdStr, 'tf_word, 'tf_count)
      .write(Tsv("foo"))


  /*val totalWordCountPerDoc = wordCountInDoc
      .groupBy('entityIdStr) { _.sum { 'tf_count -> 'total_count } }
      .project('entityId, 'entityIdStr, 'total_count)

  val tf = wordCountInDoc
      .joinWithSmaller('entityIdStr -> 'entityIdStr, totalWordCountPerDoc)
      .map(('tf_count, 'total_count) -> 'tf_count) { x: (Double, Double) => x._1 / x._2 }
      .project('entityId, 'entityIdStr, 'tf_word, 'tf_count)

  val d = inputs
      .unique('entityIdStr)
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
      .project('entityId, 'entityIdStr, 'tf_idf, 'tf_word)
      .map(('tf_word, 'tf_idf) -> 'avro_tf_idf) {
        x: (String, Double) =>
            AvroRecord("word" -> x._1, "count" -> 0, "tf_idf" -> x._2)
      }
      .groupBy('entityIdStr) { _.toList[AvroRecord]('avro_tf_idf -> 'tf_idf_list) }
      .packAvro('tf_idf_list -> 'frequencies)
      .write(KijiOutput(args("product-table"))('frequencies -> "info:term_frequencies")) */




        /*      .map { word => word.asString() }
              .groupBy(s => s).mapValues(_.size).toList
              .map { case (w, c) => AvroRecord("word" -> w, "count" -> c) }
        } */

    /*val docFrequencies = termFrequencyList
        .flatMap('termFrequencyList -> 'word) {
            terms: List[AvroRecord] => terms.map { term: AvroRecord => term("word").asString() }
        }
        .groupBy('word) { _.size }.to


    termFrequencyList.map('termFrequencyList -> 'tfIdfList) {
      terms: List[AvroRecord] => terms.foreach {
        term: AvroRecord =>
            AvroRecord("tf_idf" -> term("count").asDouble() / docFrequencies.get(term("word").asString()),
                       "word" -> term("word"), "count" -> term("count"))
      }
    }.packAvro('tfIdfList -> 'tfIdfs)
    .write(KijiOutput(args("product-table"))('tfIdfs -> "info:term_frequencies"))*/
}
