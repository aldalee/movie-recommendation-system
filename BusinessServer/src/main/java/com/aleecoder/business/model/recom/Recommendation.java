package com.aleecoder.business.model.recom;

/**
 * 推荐项目的包装
 * @author HuanyuLee
 */
public class Recommendation {
    private int mid;
    private Double score;

    public Recommendation() {
    }

    public Recommendation(int mid, Double score) {
        this.mid = mid;
        this.score = score;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}
