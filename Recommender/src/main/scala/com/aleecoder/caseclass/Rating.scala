package com.aleecoder.caseclass

/**
  * 评分样例类
  * @param uid       用户ID
  * @param mid       电影ID
  * @param score     电影评分
  * @param timestamp 时间戳
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
