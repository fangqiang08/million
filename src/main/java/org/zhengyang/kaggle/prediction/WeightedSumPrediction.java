package org.zhengyang.kaggle.prediction;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.similarity.Similarity;

import com.google.inject.Inject;

/**
 * The formula can be found in <<Item-Based Collaborative Filtering Recommendation Algorithms>>
 * @author Zhengyang
 *
 */
public class WeightedSumPrediction implements PredictionVal {
  static Logger logger = Logger.getLogger(WeightedSumPrediction.class);
  private Similarity similarityCalculator;
  private Query queryEngine;
  
  @Inject
  public WeightedSumPrediction(Similarity similarityCalculator, Query queryEngine) {
    this.similarityCalculator = similarityCalculator;
    this.queryEngine = queryEngine;
  }

  public double getPredictionVal(String userId, String songId) {
    // logger.debug("Calculating prediction value for user:" + userId + ", and song:" + songId);
    String[] listenedSongs = queryEngine.listenedSongs(userId);
    double val =  numerator(songId, userId, listenedSongs) / denominator(songId, listenedSongs);
    // logger.debug("Calculating prediction value finished.");
    return val;
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
