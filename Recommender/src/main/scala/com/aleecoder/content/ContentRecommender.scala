package com.aleecoder.content

import com.aleecoder.caseclass.Movie
import com.aleecoder.configs.ConfigClass.{getSparkSession, mongoConfig, storeDFInMongoDB}
import com.aleecoder.offline.OfflineRecommender.getMovieSimilarity
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.jblas.DoubleMatrix

/**
  * 基于内容的推荐服务
  * @author HuanyuLee
  * @date 2022/4/24
  */
object ContentRecommender {
    // 定义表名和常量
    val MONGODB_MOVIE_COLLECTION = "Movie"
    val CONTENT_MOVIE_RECS = "ContentMovieRecs"

    def main(args: Array[String]): Unit = {
        val spark = getSparkSession("ContentRecommender")
        spark.sparkContext.setLogLevel("error")
        import spark.implicits._
        /*
        加载数据，并做预处理
         */
        val movieTagsDF = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .map(x =>
                // 提取mid，name，genres三项作为原始内容特征，分词器默认按照空格做分词
                (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c))
            )
            .toDF("mid", "name", "genres")
            .cache()

        /*
        利用HashingTF工具，统计一个词语序列对应的词频
         */
        // 创建一个分词器，默认按空格分词，对原始数据做转换，生成新的一列words
        val tokenizer = new Tokenizer()
            .setInputCol("genres")
            .setOutputCol("words")
            .transform(movieTagsDF)
        // 引入HashingTF工具，可以把一个词语序列转化成对应的词频
        val featurizedData = new HashingTF()
            .setInputCol("words")
            .setOutputCol("rawFeatures")
            .setNumFeatures(50)
            .transform(tokenizer)

        //featurizedData.show(truncate = false)

        /*
        用TF-IDF从内容信息中提取电影特征向量
         */
        // 引入IDF工具，可以得到idf模型
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        // 训练idf模型，得到每个词的逆文档频率
        val idfModel = idf.fit(featurizedData)
        // 用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
        val rescaledData = idfModel.transform(featurizedData)

        rescaledData.show(truncate = false)

        val movieFeatures = rescaledData
            .map(row =>
                (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
            )
            .rdd
            .map(x => (x._1, new DoubleMatrix(x._2)))
        //movieFeatures.collect().foreach(println)

        val movieRecs = getMovieSimilarity(movieFeatures).toDF()

        storeDFInMongoDB(movieRecs, CONTENT_MOVIE_RECS)

        spark.stop()
    }

}
