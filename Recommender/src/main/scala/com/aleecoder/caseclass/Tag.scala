package com.aleecoder.caseclass

/**
  * 标签样例类
  * @param uid       用户ID
  * @param mid       电影ID
  * @param tag       电影标签
  * @param timestamp 时间戳
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)
