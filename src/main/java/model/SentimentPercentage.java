package model;

public class SentimentPercentage extends SentimentPercent{

  public SentimentPercentage(double negativePercent, double positivePercent,
      double neutralPercent) {
    super(negativePercent, positivePercent, neutralPercent);
  }

  @Override
  public String toString() {
    return String.format("%.2f %s, %.2f %s, %.2f %s.",getNegativePercent(),"% Negative",
        getNeutralPercent(),"% Neutral",getPositivePercent(),"% Positive");
  }
}
