package org.zhengyang.kaggle.distributed;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.DistributedCFModule;
import org.zhengyang.kaggle.io.OutputFormatter;
import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.internal.util.Sets;

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
  public int numberOfPopSongs = 1000;
  public int numberOfSongRecommended = 2;
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
    logger.debug("set num of pop songs:" + numberOfPopSongs);
    this.numberOfPopSongs = numberOfPopSongs;
  }
  
  public void setNumberOfSongRecommended(int number) {
    numberOfSongRecommended = number;
  }
  
  public void action() throws InterruptedException {
    logger.info("Worker has started to listen to the WORKING_Q");
    while (!stop) {
      String command = jedisConnector.jedis().lpop(Constants.COMMAND_Q);
      if (command != null && !command.equals("")) {
        // TODO : take the command
        break;
      }
      String userId = jedisConnector.jedis().blpop(0, Constants.WORKING_Q).get(1);
      logger.info("Get task: " + userId + ", start computing.");
      String[] recommendations = makeRecommendation(userId);
      if (recommendations.length < numberOfSongRecommended) {
        logger.warn("Need " + numberOfSongRecommended + " songs, only have " + recommendations.length + " , will pick some from TopN list...");
        recommendations = getRecommendationsWithPopSongs(recommendations, numberOfSongRecommended);
      }
      String result = outputFormatter.formatRecommendation(recommendations);
      logger.info("Finished recommending " + recommendations.length  + " songs for user: " + userId + ", saving it to database");;
      saveResult(userId, result);
    }
    logger.info("Worker stopped.");
  }

  private void saveResult(String userId, String result) throws InterruptedException {
    try {
      jedisConnector.jedis().lpush(Constants.RESULT_KEY_Q, userId);
      jedisConnector.jedis().hset(Constants.RESULT_HASH, userId, result);
    } catch (Exception e) {
      logger.info("An exception happened while trying to save result to redis, will try after 1 sec.");
      Thread.sleep(1000);
      saveResult(userId, result);
    }
  }

  private String[] getRecommendationsWithPopSongs(String[] recommendations, int numberOfSongRecommended) {
    Set<String> rec = Sets.newHashSet();
    rec.addAll(Arrays.asList(recommendations));
    for (String popSong : queryEngine.mostPopularSongs(numberOfPopSongs)) {
      if (rec.size() == numberOfSongRecommended)
        break;
      rec.add(popSong);
    }
    return rec.toArray(new String[0]);
  }
  
  public void stop() {
    stop = true;
  }
  
  /**
   * @param userId
   * @return
   */
  private String[] makeRecommendation(String userId) {
    logger.debug("num of pop songs: " + numberOfPopSongs);
    Map<String, Double> songScoreMap = Maps.newHashMap();
    for (String song : queryEngine.possibleRecommendation(userId, numberOfPopSongs)) {   
      double score = predictionValCalculator.getPredictionVal(userId, song);        
      songScoreMap.put(song, score);
    }
    String[] recommendation = Utils.getTopElementsOf(Utils.getSortedKeys(songScoreMap), numberOfSongRecommended);
    return recommendation;
  }

  public static void main(String[] args) throws InterruptedException {
    Worker w = Guice.createInjector(new DistributedCFModule()).getInstance(Worker.class);
    w.setNumberOfPopSongs(3);
    w.setNumberOfSongRecommended(2);
    w.action();
  }

  public void run() {
    try {
      action();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
