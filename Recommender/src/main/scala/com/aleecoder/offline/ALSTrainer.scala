package com.aleecoder.offline

import breeze.numerics.sqrt
import com.aleecoder.caseclass.MovieRating
import com.aleecoder.configs.ConfigClass.getSparkSession
import com.aleecoder.configs.ConfigClass.mongoConfig
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import com.aleecoder.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.rdd.RDD

/**
  * 模型评估和参数选取
  * @author HuanyuLee
  * @date 2022/4/19
  */
object ALSTrainer {
    /**
      * 模型调优
      * @param trainData 训练数据集
      * @param testData  测试数据集
      */
    def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
        val result = for (rank <- Array(50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
        //val result = for (rank <- 10 to 1000 by 10; lambda <- 0.01f to 1f by 0.01f)
            yield {
                val model = ALS.train(trainData, rank, 5, lambda)
                // 计算当前参数对应模型的rmse，返回Double
                val rmse = getRMSE(model, testData)
                (rank, lambda, rmse)
            }
        // 控制台打印输出最优参数
        println(result.minBy(_._3))
    }

    /**
      * 均方根误差
      * @param model 模型
      * @param data 数据
      * @return
      */
    def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
        // 计算预测评分
        val userProducts = data.map(item => (item.user, item.product))
        val predictRating = model.predict(userProducts)

        // 以uid，mid作为外键，inner join实际观测值和预测值
        val observed = data.map(item => ((item.user, item.product), item.rating))
        val predict = predictRating.map(item => ((item.user, item.product), item.rating))
        // 内连接得到(uid, mid),(actual, predict)
        sqrt(
            observed
                .join(predict)
                .map {
                    case ((uid, mid), (actual, pre)) =>
                        val err = actual - pre
                        err * err
                }.mean()
        )
    }

    def main(args: Array[String]): Unit = {
        val spark = getSparkSession("ALSTrainer")
        spark.sparkContext.setLogLevel("error")
        import spark.implicits._

        /*
        加载评分数据
         */
        val ratingRDD: RDD[Rating] = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRating]
            .rdd
            .map(rating => Rating(rating.uid, rating.mid, rating.score)) // 转化成rdd，并且去掉时间戳
            .cache()

        /*
        随机切分数据集，生成训练集和测试集
         */
        val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
        val trainingRDD: RDD[Rating] = splits(0)
        val testRDD: RDD[Rating] = splits(1)

        /*
        模型参数选择，输出最优参数
         */
        adjustALSParam(trainingRDD, testRDD)

        spark.close()
    }
}
