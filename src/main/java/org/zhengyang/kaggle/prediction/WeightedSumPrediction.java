package org.zhengyang.kaggle.prediction;

import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.similarity.Similarity;

import com.google.inject.Inject;

/**
 * The formula can be found in <<Item-Based Collaborative Filtering Recommendation Algorithms>>
 * @author Zhengyang
 *
 */
public class WeightedSumPrediction implements PredictionVal {
  private Similarity similarityCalculator;
  private Query queryEngine;
  
  @Inject
  public WeightedSumPrediction(Similarity similarityCalculator, Query queryEngine) {
    this.similarityCalculator = similarityCalculator;
    this.queryEngine = queryEngine;
  }

  public double getPredictionVal(String userId, String songId) {
    String[] listenedSongs = queryEngine.listenedSongs(userId);
    return numerator(songId, userId, listenedSongs) / denominator(songId, listenedSongs);
  }
  
  protected double numerator(String songId, String userId, String[] listenedSongs) {
    double sum = 0;
    for (String songHasListened : listenedSongs) {
      double similarity = similarityCalculator.getSimilarity(songId, songHasListened);
      double rating = queryEngine.getRating(userId, songHasListened);
      sum += similarity * rating;
    }
    return sum;
  }
  
  protected double denominator(String songId, String[] listenedSongs) {
    double sum = 0;
    for (String songHasListened : listenedSongs) {
      double similarity = similarityCalculator.getSimilarity(songId, songHasListened);
      sum += abs(similarity);
    }
    return sum;
  }
  
  protected double abs(double a) { return a > 0? a : -a; }
}
