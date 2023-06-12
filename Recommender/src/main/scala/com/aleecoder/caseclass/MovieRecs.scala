package com.aleecoder.caseclass

/**
  * 定义基于隐语义模型（LFM）电影特征向量的电影相似度列表
  * 定义基于电影内容信息提取出的特征向量的电影相似度列表
  * @param mid
  * @param recs
  */
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
