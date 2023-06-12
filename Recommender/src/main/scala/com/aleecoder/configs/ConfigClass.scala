package com.aleecoder.configs

import com.aleecoder.caseclass.MongoConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 定义用到的配置参数
  * @author HuanyuLee
  * @date 2022/4/19
  */
object ConfigClass {
    val config = Map(
        "spark.cores" -> "local[*]",
        "mongo.uri" -> "mongodb://192.168.244.11:27017/recommender",
        "mongo.db" -> "recommender",
        "es.httpHosts" -> "localhost:9200",
        "es.transportHosts" -> "localhost:9300",
        "es.index" -> "recommender",
        "es.cluster.name" -> "elasticsearch",
        "kafka.topic" -> "recommender"
    )

    /*
    定义kafka连接参数
    */
    val kafkaParam = Map(
        "bootstrap.servers" -> "node02:9092", // Kafka集群
        "key.deserializer" -> classOf[StringDeserializer], // key的反序列化
        "value.deserializer" -> classOf[StringDeserializer], // value的反序列化
        "group.id" -> "recommender",
        "auto.offset.reset" -> "latest" // 偏移量初始设置
    )

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    /**
      * 生成SparkSession对象
      * @param appName 应用的名字
      * @return SparkSession对象
      */
    def getSparkSession(appName: String): SparkSession = {
        val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName(appName)
        SparkSession.builder().config(sparkConf).getOrCreate()
    }

    /**
      * 将结果保存到MongoDB
      * @param df              DataFrame
      * @param collection_name collection名字
      * @param mongoConfig     MongoDB配置
      */
    def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
        df.write
            .option("uri", mongoConfig.uri)
            .option("collection", collection_name)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
    }
}
