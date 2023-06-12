package com.aleecoder.dataloader

import com.aleecoder.caseclass._
import com.aleecoder.configs.ConfigClass.{config, getSparkSession}
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

/**
  * 数据加载
  * @author HuanyuLee
  * @date 2022/4/18
  */

object DataLoader {
    val MOVIE_DATA_PATH = "./Recommender/src/main/resources/movies.csv"
    val RATING_DATA_PATH = "./Recommender/src/main/resources/ratings.csv"
    val TAG_DATA_PATH = "./Recommender/src/main/resources/tags.csv"
    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"

    /**
      * 将结果保存到MongoDB
      * @param movieDF     电影的DataFrame
      * @param ratingDF    评分的DataFrame
      * @param tagDF       标签的DataFrame
      * @param mongoConfig MongoDB的配置
      */
    def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
        // 新建一个MongoDB的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        // 如果MongoDB中已经有相应的数据库，先删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        // 将DF数据写入对应的MongoDB表中
        movieDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        ratingDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        tagDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

        //关闭 MongoDB 的连接
        mongoClient.close()
    }

    /**
      * 将结果保存到ES
      * @param movieDF  电影的DataFrame
      * @param esConfig ES的配置
      */
    def scoreDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
        //新建一个配置
        val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()
        //新建一个 ES 的客户端
        val esClient = new PreBuiltTransportClient(settings)
        //需要将 TransportHosts 添加到 esClient 中
        val REGEX_HOST_PORT = "(.+):(\\d+)".r
        esConfig.transportHosts.split(",").foreach {
            case REGEX_HOST_PORT(host: String, port: String) =>
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
        }
        // 需要清除掉 ES 中遗留的数据
        if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
            esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
        }
        esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
        // 将数据写入到 ES 中
        movieDF.write
            .option("es.nodes", esConfig.httpHosts)
            .option("es.http.timeout", "100m")
            .option("es.mapping.id", "mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(esConfig.index + "/" + ES_MOVIE_INDEX)

    }

    def main(args: Array[String]): Unit = {
        val spark = getSparkSession("DataLoader")
        import spark.implicits._

        // 加载数据
        // 将 Movie、Rating、Tag 数据集加载进来
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        //将 MovieRDD 装换为 DataFrame
        val movieDF = movieRDD.map(
            item => {
                val attr = item.split("\\^")
                Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
            }).toDF()

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

        //将 ratingRDD 转换为 DataFrame
        val ratingDF = ratingRDD.map(
            item => {
                val attr = item.split(",")
                Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
            }).toDF()
        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

        //将 tagRDD 装换为 DataFrame
        val tagDF = tagRDD.map(
            item => {
                val attr = item.split(",")
                Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
            }).toDF()


        // 声明一个隐式的配置对象
        implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        // 将数据保存到MongoDB
        storeDataInMongoDB(movieDF, ratingDF, tagDF)

        // 数据预处理    把movie对应的tag信息添加进去，加一列 tag1|tag2|...
        val newTag = tagDF.groupBy($"mid")
            .agg(concat_ws("|", collect_set($"tag")).as("tags"))
            .select("mid", "tags")

        // newTag和movie做join，数据合并在一起
        val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

        // 声明一个隐式的配置对象
        implicit val esConfig: ESConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
        // 保存数据到ES
        // Exception in thread "main" java.lang.IllegalStateException: availableProcessors is already set to [16], rejecting [16]
        // netty相关jar冲突
        System.setProperty("es.set.netty.runtime.available.processors", "false")
        scoreDataInES(movieWithTagsDF)

        spark.stop()
    }
}
