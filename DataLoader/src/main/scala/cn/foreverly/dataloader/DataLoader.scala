package cn.foreverly.dataloader

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala._
/**
 * Product数据集
 * 3982                            商品ID
 * Fuhlen 富勒 M8眩光舞者时尚节能    商品名称
 * 1057,439,736                    商品分类ID，不需要
 * B009EJN4T2                      亚马逊ID，不需要
 * https://images-cn-4.ssl-image   商品的图片URL
 * 外设产品|鼠标|电脑/办公           商品分类
 * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 * Product数据集
 * 3982                            商品ID
 * Fuhlen 富勒 M8眩光舞者时尚节能     商品名称
 * 1057,439,736                    商品分类ID，不需要
 * B009EJN4T2                      亚马逊ID，不需要
 * https://images-cn-4.ssl-image   商品的图片URL
 * 外设产品|鼠标|电脑/办公           商品分类
 * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
 */
case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)

object DataLoader {
  val PRODUCT_INPUT = "/home/hadoop/IdeaProjects/ERecommend/DataLoader/src/main/resources/products.csv"
  val RATING_INPUT = "/home/hadoop/IdeaProjects/ERecommend/DataLoader/src/main/resources/ratings.csv"

  val URL = "mongodb://localhost"
  val DB = "recommender"
  val RatingCollection = "rating"
  val ProductCollection = "product"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("DataLoader").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val ratingDF = sc.textFile(RATING_INPUT).map { line =>
      val sp = line.split(",")
      Rating(sp(0).toInt, sp(1).toInt, sp(2).toDouble, sp(3).toInt)
    }.toDF()

    val productDF = sc.textFile(PRODUCT_INPUT).map { item =>
      val attr = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }.toDF()


    val ratingWriteConfig = WriteConfig(
      Map(
        "uri" -> URL,
        "database" -> DB,
        "collection" -> RatingCollection
      )
    )
    MongoSpark.save(ratingDF, ratingWriteConfig)

    val productWriteConfig = WriteConfig(
      Map(
        "uri" -> URL,
        "database" -> DB,
        "collection" -> ProductCollection
      )
    )
    MongoSpark.save(productDF, productWriteConfig)
    spark.stop()

    val mongoClient: MongoClient = MongoClient()
    val recommender: MongoDatabase = mongoClient.getDatabase(DB)
    val rating: MongoCollection[Document] = recommender.getCollection(RatingCollection)
    import Helpers._
    rating.createIndex(Indexes.ascending("userId")).printResults()
    val product: MongoCollection[Document] = recommender.getCollection(ProductCollection)
    product.createIndex(Indexes.ascending("productId")).printResults()
  }
}
