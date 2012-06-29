package org.zhengyang.kaggle.distributed;

import java.util.Map;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.JedisTestCFModule;
import org.zhengyang.kaggle.io.OutputFormatter;
import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Inject;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jun 29, 2012
 */
public class Worker implements Runnable {
  static Logger logger = Logger.getLogger(Worker.class);
  private Query queryEngine;
  private JedisConnector jedisConnector;
  private PredictionVal predictionValCalculator;
  private OutputFormatter outputFormatter;
  private int numberOfPopSongs = 1000;
  private int numberOfSongRecommended = 2;
  private boolean stop = false;
  
  @Inject
  public Worker(Query queryEngine, 
      JedisConnector jedisConnector, 
      PredictionVal predictionValCalculator,
      OutputFormatter outputFormatter) {
    this.queryEngine = queryEngine;
    this.jedisConnector = jedisConnector;
    this.predictionValCalculator = predictionValCalculator;
    this.outputFormatter = outputFormatter;
  }
  
  public void setNumberOfPopSongs(int numberOfPopSongs) {
    this.numberOfPopSongs = numberOfPopSongs;
  }
  
  public void setNumberOfSongRecommended(int number) {
    numberOfSongRecommended = number;
  }
  
  public void action() {
    logger.info("Worker has started to listen to the WORKING_Q");
    while (!stop) {
      String command = jedisConnector.jedis().lpop(Constants.COMMAND_Q);
      if (command != null && !command.equals("")) {
        // TODO : take the command
        break;
      }
      String userId = jedisConnector.jedis().blpop(0, Constants.WORKING_Q).get(1);
      logger.info("Get task: " + userId + ", start computing.");
      String result = outputFormatter.formatRecommendation(makeRecommendation(userId));
      logger.info("Finished computing for task: " + userId + ", saving it to database");;
      jedisConnector.jedis().lpush(Constants.RESULT_KEY_Q, userId);
      jedisConnector.jedis().hset(Constants.RESULT_HASH, userId, result);
    }
    logger.info("Worker stopped.");
  }
  
  public void stop() {
    stop = true;
  }
  
  /**
   * @param userId
   * @return
   */
  private String[] makeRecommendation(String userId) {
    Map<String, Double> songScoreMap = Maps.newHashMap();
    for (String song : queryEngine.possibleRecommendation(userId, numberOfPopSongs)) {   
      double score = predictionValCalculator.getPredictionVal(userId, song);        
      songScoreMap.put(song, score);
    }
    String[] recommendation = Utils.getTopElementsOf(Utils.getSortedKeys(songScoreMap), numberOfSongRecommended);
    return recommendation;
  }

  public static void main(String[] args) {
    Worker w = Guice.createInjector(new JedisTestCFModule()).getInstance(Worker.class);
    w.setNumberOfPopSongs(3);
    w.setNumberOfSongRecommended(2);
    w.action();
  }

  public void run() {
    action();
  }
}
