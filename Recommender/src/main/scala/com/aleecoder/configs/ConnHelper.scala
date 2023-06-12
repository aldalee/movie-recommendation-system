package com.aleecoder.configs

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

/**
  * 定义连接助手对象，序列化
  * @author HuanyuLee
  * @date 2022/4/22
  */
object ConnHelper extends Serializable {
    lazy val jedis = new Jedis("192.168.244.11", 6379)
    lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://192.168.244.11:27017/recommender"))
}
