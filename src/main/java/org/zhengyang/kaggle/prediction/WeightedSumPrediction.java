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
    double sumOfNumerator = 0;
    double sumOfDenominator = 0;
    for (String listenedSong : listenedSongs) {
      double similarity = similarityCalculator.getSimilarity(songId, listenedSong);
      double rating = queryEngine.getRating(userId, listenedSong);
      sumOfNumerator += similarity * rating;
      sumOfDenominator += abs(similarity);
    }
    // logger.debug("Calculating prediction value finished.");
    return sumOfNumerator / sumOfDenominator;   
  }
 
  protected double abs(double a) { return a > 0? a : -a; }
}
