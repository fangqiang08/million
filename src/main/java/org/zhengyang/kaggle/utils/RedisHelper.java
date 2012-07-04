package org.zhengyang.kaggle.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.distributed.Constants;
import org.zhengyang.kaggle.distributed.JedisConnector;
import org.zhengyang.kaggle.inject.DistributedCFModule;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.inject.Guice;
import com.google.inject.Inject;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jul 3, 2012
 */
public class RedisHelper {
  static Logger logger = Logger.getLogger(RedisHelper.class);
  private JedisConnector jedis;

  @Inject
  public RedisHelper(JedisConnector jedis) {
    this.jedis = jedis;
  }
  
  private Jedis jedis() {
    return jedis.jedis();
  }
  
  public void loadColistenedMatrix(String pathToColistenedMatrix) throws IOException, InterruptedException {
    logger.info("Start loading colistened matrix from " + pathToColistenedMatrix + " to redis: " + Constants.COLISTENED_HASH);
    BufferedReader br = new BufferedReader(new FileReader(new File(pathToColistenedMatrix)));
    String line = null;
    int lineCount = 0;
    while ((line = br.readLine()) != null) {
      lineCount ++;
      String song1 = line.split("\\s+")[0];
      String song2 = line.split("\\s+")[1];
      int count    = Integer.valueOf(line.split("\\s+")[2]);
      putEntryToRedis(song1, song2, count);
      if (lineCount % 10000 == 0) {
        logger.info("finished line: " + lineCount);
      }
    }
    logger.info("Colistened matrix has been loaded into redis, size: " + jedis().hlen(Constants.COLISTENED_HASH));
  }

  private void putEntryToRedis(String song1, String song2, int count) throws InterruptedException {
    try {
      if (!(jedis().hget(Constants.COLISTENED_HASH, song1 + "-" + song2) == null)) {
        int currentCount = Integer.valueOf(jedis().hget(Constants.COLISTENED_HASH, song1 + "-" + song2));
        jedis().hset(Constants.COLISTENED_HASH, song1 + "-" + song2, currentCount + 1 + "");
      } else if (!(jedis().hget(Constants.COLISTENED_HASH, song2 + "-" + song1) == null)) {
        int currentCount = Integer.valueOf(jedis().hget(Constants.COLISTENED_HASH, song2 + "-" + song1));
        jedis().hset(Constants.COLISTENED_HASH, song2 + "-" + song1, currentCount + 1 + "");
      } else {
        jedis().hset(Constants.COLISTENED_HASH, song1 + "-" + song2, String.valueOf(1));
      }
    } catch (JedisConnectionException e) {
      logger.warn("Jedis Connection Timeout, will retry in 1 second.");
      Thread.sleep(1000);
      putEntryToRedis(song1, song2, count);
    }   
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    Guice.createInjector(new DistributedCFModule()).getInstance(RedisHelper.class).loadColistenedMatrix("data/shared/colisten-matrix.txt");
  }
}
