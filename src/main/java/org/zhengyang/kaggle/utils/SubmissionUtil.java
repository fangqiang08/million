package org.zhengyang.kaggle.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.distributed.Constants;
import org.zhengyang.kaggle.distributed.JedisConnector;
import org.zhengyang.kaggle.inject.JedisTestCFModule;

import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.internal.util.Lists;
import com.google.inject.internal.util.Maps;

public class SubmissionUtil {
  static Logger logger = Logger.getLogger(SubmissionUtil.class);
  private JedisConnector jedisConnector;
  private List<String> allUsers;
  private Map<String, Integer> songIdMap;
  private Joiner joiner = Joiner.on(",").skipNulls();
  
  @Inject
  public SubmissionUtil(JedisConnector jedisConnector) {
    this.jedisConnector = jedisConnector;
  }
  
  public void init(String pathToSongs, String pathToUsers) throws IOException {
    allUsers = Lists.newArrayList();
    songIdMap = Maps.newHashMap();
    logger.info("Building SongIdMap...");
    BufferedReader songReader = new BufferedReader(new FileReader(new File(pathToSongs)));
    String line = null;
    while((line = songReader.readLine()) != null) {
      songIdMap.put(line.split("\\s+")[0], Integer.valueOf(line.split("\\s+")[1]));
    }
    logger.info("Building SongIdMap finished, number of songs:" + songIdMap.size());
    logger.info("Building users list...");
    BufferedReader userReader = new BufferedReader(new FileReader(new File(pathToUsers)));
    line = null;
    while((line = userReader.readLine()) != null) {
      allUsers.add(line);
    }
    logger.info("Building allUsers finished, number of users:" + allUsers.size());
  }
  
  /**
   * 
   * @param filePath path of the result file.
   * @throws IOException 
   */
  public void generateResult(String filePath) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath)));
    int total = allUsers.size();
    int count = 0;
    for (int i = 0; i < allUsers.size(); i++) {
      String[] recommendations = jedisConnector.jedis().hget(Constants.RESULT_HASH, allUsers.get(i)).split("\\s+");
      List<Integer> recommendationIds = Lists.newArrayList();
      for (String song : recommendations) {
        int index = songIdMap.get(song);
        recommendationIds.add(index);
      }
      bw.write(joiner.join(recommendationIds) + "\n");
      count ++;
      if (count % 100 == 0) {
        logger.info("Finished: " + count + "/" + total);
      }
    }
    bw.close();
  }
  
  public static void main(String[] args) throws IOException {
    SubmissionUtil util = Guice.createInjector(new JedisTestCFModule()).getInstance(SubmissionUtil.class);
    util.init("data/kaggle_songs.txt", "data/kaggle_users.txt");
    util.generateResult("data/20120702/result.csv");
  }
}
