package org.zhengyang.kaggle.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

public class CollaborativeFilter {
  // component of this algorithm 
  private Query queryEngine;
  private PredictionVal predictionValCalculator;
  
  // parameters of this algorithm
  private int numberOfPopSongs;
  private int numberOfSongRecommended;
  
  // variables used by algorithm
  private Map<String, String[]> userRecommendation = Maps.newHashMap();
  
  @Inject
  public CollaborativeFilter(
      Query queryEngine, 
      PredictionVal predictionValCalculator,
      @Assisted("numberOfPopSongs") int numberOfPopSongs,
      @Assisted("numberOfSongRecommended") int numberOfSongRecommended) {
    this.queryEngine = queryEngine;
    this.predictionValCalculator = predictionValCalculator;
  }

  public void run() {
    // recommend songs to each user
    for (String userId : queryEngine.allUsers()) {
      Map<String, Double> songScoreMap = Maps.newHashMap();
      for (String song : possibleRecommendation(userId)) {
        double score = predictionValCalculator.getPredictionVal(userId, song);
        songScoreMap.put(song, score);
      }
      // take the songs with highest prediction value, number of songs is specified in numberOfSongRecommended
      String[] recommendation = Utils.getTopElementsOf(Utils.getSortedKeys(songScoreMap), numberOfSongRecommended);
      // check if there are enough songs for this user
      if (recommendation.length < numberOfSongRecommended) {
        // TODO : log this warning, and put most popular songs that user has not listened yet
      }
      userRecommendation.put(userId, recommendation);
    }
  }
  
  /**
   * Possible recommendation: 1, popular songs 2, user has not listened
   * @param userId
   * @return array of possible recommended songs' id
   */
  private String[] possibleRecommendation(String userId) {
    List<String> allSongs = Lists.newArrayList();
    allSongs.addAll(Arrays.asList(queryEngine.mostPopularSongs(numberOfPopSongs)));
    allSongs.addAll(Arrays.asList(queryEngine.listenedSongs(userId)));
    return Sets.newHashSet(allSongs).toArray(new String[0]);
  }
}
 
