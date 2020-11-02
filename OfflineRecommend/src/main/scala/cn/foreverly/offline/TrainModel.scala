package cn.foreverly.offline

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

case class ProductRec(productId: Int, productCosine: List[Recommendation])

object TrainModel {
  val SPARK_MASTER = "local[2]"

  val URI = "mongodb://localhost"
  val DATABASE = "recommender"
  val RATING_COLLECTION = "rating"
  val PRODUCT_COLLECTION = "product"


  // 定义模型的训练参数
  val RANK = 50
  val ITERATIONS = 10
  val REG = 0.01

  val Thresh = 0.6

  def read(spark: SparkSession, uri: String, database: String, collection: String): DataFrame = {
    val readConfig = ReadConfig(Map(
      "uri" -> uri,
      "database" -> database,
      "collection" -> collection
    ))
     MongoSpark.load(spark, readConfig)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master(SPARK_MASTER)
      .appName("trainModel")
      .getOrCreate()
    // 读取ratingDF并转换数据类型
    val ratingDF = read(spark, URI, DATABASE, RATING_COLLECTION)
//    ratingDF.printSchema()
//    ratingDF.show(3,truncate = false)


    val als = new ALS()
      .setMaxIter(ITERATIONS)
      .setRank(RANK)
      .setRegParam(REG)
      .setUserCol("userId")
      .setItemCol("productId")
      .setRatingCol("score").setNonnegative(false)
    val model = als.fit(ratingDF)
    model.save("/home/hadoop/IdeaProjects/ERecommend/OfflineRecommend/src/main/resources/aslModel")


  }
}
