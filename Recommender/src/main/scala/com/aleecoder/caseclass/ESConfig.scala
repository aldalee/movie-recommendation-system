package com.aleecoder.caseclass

/**
  * ESConfig样例类
  * @param httpHosts
  * @param transportHosts
  * @param index
  * @param clusterName
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)
