package spark.jobs.adapter.spark.ml

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.{LogisticRegression => SparkLogisticRegression}
import org.apache.spark.ml.recommendation.{ALS => SparkALS}
import org.apache.spark.ml.feature.{
  Binarizer => SparkBinarizer,
  HashingTF => SparkHashingTF,
  IDF => SparkIDF,
  RegexTokenizer => SparkRegexTokenizer,
  StopWordsRemover => SparkStopWordsRemover,
  StringIndexer => SparkStringIndexer
}

sealed trait SparkMLComponent { self =>
  def inner: PipelineStage
  def ~>(that: SparkMLComponent): Array[SparkMLComponent] =
    Array(self, that)
}

object SparkMLComponent {
  final case class IDF private (inner: SparkIDF)                           extends SparkMLComponent
  final case class HashingTF private (inner: SparkHashingTF)               extends SparkMLComponent
  final case class Binarizer private (inner: SparkBinarizer)               extends SparkMLComponent
  final case class StringIndexer private (inner: SparkStringIndexer)       extends SparkMLComponent
  final case class RegexTokenizer private (inner: SparkRegexTokenizer)     extends SparkMLComponent
  final case class StopWordsRemover private (inner: SparkStopWordsRemover) extends SparkMLComponent

  final case class ALSRecommender(inner: SparkALS)                    extends SparkMLComponent
  final case class LogisticRegression(inner: SparkLogisticRegression) extends SparkMLComponent

  def idf(minDocFreq: Int = 5, inputCol: String, outputCol: String): SparkMLComponent =
    IDF(
      new SparkIDF()
        .setInputCol(inputCol)
        .setOutputCol(outputCol)
        .setMinDocFreq(minDocFreq)
    )

  def hashingTF(numFeatures: Int = 5000, inputCol: String, outputCol: String): SparkMLComponent =
    HashingTF(
      new SparkHashingTF()
        .setInputCol(inputCol)
        .setOutputCol(outputCol)
        .setNumFeatures(numFeatures)
    )

  def binarizer(threshold: Int = 3, inputCol: String, outputCol: String): SparkMLComponent =
    Binarizer(
      new SparkBinarizer()
        .setThreshold(threshold)
        .setInputCol(inputCol)
        .setOutputCol(outputCol)
    )

  def stringIndexer(inputCol: String, outputCol: String): SparkMLComponent =
    StringIndexer(
      new SparkStringIndexer()
        .setHandleInvalid("keep")
        .setInputCol(inputCol)
        .setOutputCol(outputCol)
    )

  def stopWordsRemover(language: String = "english", inputCol: String): SparkMLComponent =
    StopWordsRemover(
      new SparkStopWordsRemover()
        .setStopWords(SparkStopWordsRemover.loadDefaultStopWords(language))
        .setInputCol(inputCol)
    )

  def tokenizer(toLowercase: Boolean = true, inputCol: String, outputCol: String): SparkMLComponent =
    RegexTokenizer(
      new SparkRegexTokenizer()
        .setPattern("[\\W_]+")
        .setToLowercase(toLowercase)
        .setMinTokenLength(4)
        .setInputCol(inputCol)
        .setOutputCol(outputCol)
    )

  def logisticRegression(featuresCol: String, labelCol: String, maxIterations: Int): SparkMLComponent =
    LogisticRegression(
      new SparkLogisticRegression()
        .setFeaturesCol(featuresCol)
        .setLabelCol(labelCol)
        .setMaxIter(maxIterations)
    )

  def alsRecommender(
    maxIteration: Int,
    regParam: Double = 0.2,
    userCol: String,
    itemCol: String,
    ratingCol: String
  ): SparkMLComponent =
    ALSRecommender(
      new SparkALS()
        .setMaxIter(maxIteration)
        .setRegParam(regParam)
        .setNonnegative(true)
        .setColdStartStrategy("drop")
        .setUserCol(userCol)
        .setItemCol(itemCol)
        .setRatingCol(ratingCol)
    )
}
