package com.aleecoder.caseclass

/**
  * 定义电影类别Top10 推荐对象
  * @param genres
  * @param recs
  */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])
