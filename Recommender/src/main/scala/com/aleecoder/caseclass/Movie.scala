package com.aleecoder.caseclass

/**
  * 电影样例类
  * @param mid       电影ID
  * @param name      电影名称
  * @param descri    详情描述
  * @param timelong  时长
  * @param issue     发行时间
  * @param shoot     拍摄时间
  * @param language  语言
  * @param genres    类型
  * @param actors    演员表
  * @param directors 导演
  */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)
