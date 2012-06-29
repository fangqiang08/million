package org.zhengyang.kaggle.core;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.io.OutputFormatter;
import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

public class CollaborativeFilter {
  static Logger logger = Logger.getLogger(CollaborativeFilter.class);
  
  // component of this algorithm 
  private Query queryEngine;
  private PredictionVal predictionValCalculator;
  private OutputFormatter outputFormatter;
  
  // parameters of this algorithm
  private int numberOfPopSongs;
  private int numberOfSongRecommended;  
  
  // variables used by algorithm
  private Map<String, String[]> userRecommendation = Maps.newHashMap();
 
  @Inject
  public CollaborativeFilter(
      Query queryEngine, 
      PredictionVal predictionValCalculator,
      OutputFormatter outputFormatter,
      @Assisted("numberOfPopSongs") int numberOfPopSongs,
      @Assisted("numberOfSongRecommended") int numberOfSongRecommended,
      @Assisted("outputPath") String outputPath) throws IOException {
    this.queryEngine             = queryEngine;
    this.predictionValCalculator = predictionValCalculator;
    this.outputFormatter         = outputFormatter;
    this.numberOfPopSongs        = numberOfPopSongs;
    this.numberOfSongRecommended = numberOfSongRecommended;
    outputFormatter.setOutputPath(outputPath);
  }

  public void run() throws IOException {
    // recommend songs to each user
    int i = 1;
    int totalNum = queryEngine.allUsers().length;
    for (String userId : queryEngine.allUsers()) {
      logger.debug("Start recommending songs to " + userId);
      Map<String, Double> songScoreMap = Maps.newHashMap();
      logger.debug("Number of possbile songs for user " + userId + ": " + queryEngine.possibleRecommendation(userId, numberOfPopSongs).length);      
      logger.debug("Calculating prediction values for possible songs.");
      for (String song : queryEngine.possibleRecommendation(userId, numberOfPopSongs)) {   
        double score = predictionValCalculator.getPredictionVal(userId, song);        
        songScoreMap.put(song, score);
      }
      logger.debug("Calculating finished.");
      // take the songs with highest prediction value, number of songs is specified in numberOfSongRecommended
      String[] recommendation = Utils.getTopElementsOf(Utils.getSortedKeys(songScoreMap), numberOfSongRecommended);
      // check if there are enough songs for this user
      if (recommendation.length < numberOfSongRecommended) {
        // TODO : log this warning, and put most popular songs that user has not listened yet
      }
//      userRecommendation.put(userId, recommendation);
      // write the recommendation to the file
      outputFormatter.writeLine(recommendation);
      logger.debug("Recommended " + recommendation.length + " songs to " + userId + ".");
      logger.info("Status: " + i + "/" + totalNum);
      i++;
    }
  }
  
  public void output(String path) throws IOException {
    outputFormatter.write(userRecommendation, queryEngine.allUsers());
  }
}
 
