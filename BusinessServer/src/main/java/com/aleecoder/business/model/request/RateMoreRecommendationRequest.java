package com.aleecoder.business.model.request;

/**
 * @author HuanyuLee
 */
public class RateMoreRecommendationRequest {
    private int sum;

    public RateMoreRecommendationRequest(int sum) {
        this.sum = sum;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}
