package cn.foreverly.offline

import cn.foreverly.offline.TrainModel.SPARK_MASTER
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession

/**
 * @author foreverly
 * @create 2020-11-02 下午10:45
 **/
object UseModel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("useModel")
      .getOrCreate()
    val model:ALSModel = ALSModel.load("/home/hadoop/IdeaProjects/ERecommend/OfflineRecommend/src/main/resources/aslModel")
    val ten = model.recommendForAllItems(10)
    ten.show(3,false)
  }

}
