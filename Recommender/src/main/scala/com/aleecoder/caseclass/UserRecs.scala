package com.aleecoder.caseclass

/**
  * 定义基于预测评分的用户推荐列表
  * @param uid  用户ID
  * @param recs 推荐列表
  */
case class UserRecs(uid: Int, recs: Seq[Recommendation])
