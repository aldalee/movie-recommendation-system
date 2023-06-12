package com.aleecoder.streaming

import com.aleecoder.caseclass.{MongoConfig, MovieRecs}
import com.aleecoder.configs.ConfigClass.{config, getSparkSession, kafkaParam, mongoConfig}
import com.aleecoder.configs.ConnHelper
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ArrayBuffer

/**
  * 实时推荐
  * @author HuanyuLee
  * @date 2022/4/20
  */
object StreamingRecommender {
    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

    import scala.collection.JavaConversions._ // redis操作返回的是java类，为了用map操作需要引入转换类

    /**
      * 从redis里获取当前用户最近的K次评分，保存成Array[(mid, score)]
      * @param num   用户最近的num次评分
      * @param uid   当前评分用户ID
      * @param jedis 连接Redis
      * @return
      */
    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
        // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
        jedis.lrange("uid:" + uid, 0, num - 1)
            .map { item =>
                val attr = item.split("\\:") // 具体每个评分是以冒号分隔的两个值
                (attr(0).trim.toInt, attr(1).trim.toDouble)
            }
            .toArray
    }

    /**
      * 从相似度矩阵中取出当前电影最相似的num个电影，作为备选电影，Array[mid]
      * @param num         相似电影的数量
      * @param mid         当前电影ID
      * @param uid         当前评分用户ID
      * @param simMovies   相似度矩阵
      * @param mongoConfig MongoDB配置
      * @return 过滤之后的备选电影列表
      */
    def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
        // 从相似度矩阵中拿到所有相似的电影
        val allSimMovies = simMovies(mid).toArray
        // 从mongodb中查询用户已看过的电影
        val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
            .find(MongoDBObject("uid" -> uid))
            .toArray
            .map(item => item.get("mid").toString.toInt)
        // 把看过的过滤，得到输出列表
        allSimMovies
            .filter(x => !ratingExist.contains(x._1))
            .sortWith(_._2 > _._2)
            .take(num)
            .map(x => x._1)
    }

    /**
      * 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
      * @param candidateMovies     备选电影
      * @param userRecentlyRatings 用户最近的评分
      * @param simMovies           相似度矩阵
      * @return 当前用户的实时推荐列表
      */
    def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)], simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
        // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
        val scores = ArrayBuffer[(Int, Double)]()
        // 定义一个HashMap，保存每一个备选电影的增强减弱因子
        val increaseMap = scala.collection.mutable.HashMap[Int, Int]()
        val decreaseMap = scala.collection.mutable.HashMap[Int, Int]()
        for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
            // 拿到备选电影和最近评分电影的相似度
            val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
            if (simScore > 0.7) {
                // 计算备选电影的基础推荐得分 运算符重载函数 传入的参数是一个元组
                scores += ((candidateMovie, simScore * userRecentlyRating._2))
                if (userRecentlyRating._2 > 3)
                    increaseMap(candidateMovie) = increaseMap.getOrDefault(candidateMovie, 0) + 1
                else
                    decreaseMap(candidateMovie) = decreaseMap.getOrDefault(candidateMovie, 0) + 1
            }
        }
        // 根据备选电影的mid做groupBy，根据公式去求最后的推荐评分
        scores
            .groupBy(_._1)
            .map {
                // groupBy之后得到的数据 Map( mid -> ArrayBuffer[(mid, score)] )
                case (mid, scoreList) =>
                    (mid, scoreList.map(_._2).sum / scoreList.length + log(increaseMap.getOrDefault(mid, 1)) - log(decreaseMap.getOrDefault(mid, 1)))
            }
            .toArray
            .sortWith(_._2 > _._2)
    }

    /**
      * 利用换底公式求一个数的对数，底数默认为10
      * @param m 正整数
      * @return
      */
    def log(m: Int): Double = {
        val N = 10
        math.log(m) / math.log(N)
    }


    /**
      * 获取当个电影之间的相似度
      * @param topSimMovie     候选电影
      * @param userRatingMovie 用户已经评分的电影
      * @param simMovies       电影相似度矩阵
      * @return
      */
    def getMoviesSimScore(topSimMovie: Int, userRatingMovie: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
        simMovies.get(topSimMovie) match {
            case Some(sim) =>
                sim.get(userRatingMovie) match {
                    case Some(score) => score
                    case None => 0.0
                }
            case None => 0.0
        }
    }

    /**
      * 把推荐数据保存到MongoDB
      * @param uid         用户ID
      * @param streamRecs  推荐列表
      * @param mongoConfig 配置
      */
    def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
        // 定义到StreamRecs表的连接
        val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
        // 如果表中已有uid对应的数据，则删除
        streamRecsCollection.findAndRemove(
            MongoDBObject("uid" -> uid)
        )
        // 将streamRecs数据存入表中
        streamRecsCollection.insert(
            MongoDBObject(
                "uid" -> uid,
                "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))
            )
        )
    }

    def main(args: Array[String]): Unit = {
        val spark = getSparkSession("StreamingRecommender")
        val sc = spark.sparkContext
        sc.setLogLevel("error")
        import spark.implicits._
        val ssc = new StreamingContext(sc, Seconds(2))

        /*
        加载电影相似度矩阵数据，把它广播出去
         */
        val simMovieMatrix = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRecs]
            .rdd
            .map { movieRecs => // 为了快速的查询相似度，转换成map
                (movieRecs.mid, movieRecs.recs
                    .map(x => (x.mid, x.score))
                    .toMap
                )
            }.collectAsMap()
        // 定义广播变量
        val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

        /*
        通过Kafka创建一个DStream
         */
        val kafkaDStream = KafkaUtils
            .createDirectStream[String, String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
            )

        /*
        把原始数据 UID|MID|SCORE|TIMESTAMP 转换成评分流
         */
        val ratingStream = kafkaDStream
            .map { msg =>
                val attr = msg.value().split("\\|")
                (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
            }
        /*
        继续做流式处理，核心实时算法部分
         */
        ratingStream.foreachRDD { rdds =>
            rdds.foreach {
                case (uid, mid, score, timestamp) =>
                    println("rating data coming! >>>>>>>>>>>>>>>>")
                    val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
                    val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)
                    val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
                    saveDataToMongoDB(uid, streamRecs)
            }
        }
        ssc.start()
        println(">>>>>>>>>>>>>>> streaming started!")
        ssc.awaitTermination()
    }
}
