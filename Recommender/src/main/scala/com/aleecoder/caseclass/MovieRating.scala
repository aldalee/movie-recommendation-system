package com.aleecoder.caseclass

/**
 *
 * @author HuanyuLee
 * @date 2022/4/19
 */
// 基于评分数据的LFM，只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)
