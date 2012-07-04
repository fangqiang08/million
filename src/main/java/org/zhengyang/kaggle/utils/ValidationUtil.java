package org.zhengyang.kaggle.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.DistributedCFModule;
import org.zhengyang.kaggle.query.Query;

import com.google.inject.Guice;
import com.google.inject.Inject;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jul 2, 2012
 */
public class ValidationUtil {
  static Logger logger = Logger.getLogger(ValidationUtil.class);
  private Query queryEngine;
  
  @Inject
  public ValidationUtil(Query queryEngine) {
    this.queryEngine = queryEngine;
  }
  
  public int validatePopSongs(String popSongFilePath) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File(popSongFilePath)));
    String line = null;
    String[] popularSongs = queryEngine.mostPopularSongs(getNumOfPopSongsInFile(popSongFilePath));
    int index = 0;
    int errorCount = 0;
    while ((line = br.readLine()) != null) {
      if (queryEngine.getIntegerIdOfSong(popularSongs[index]) == Integer.valueOf(line)) {
        logger.info("Rank#" + index + " Correct! QueryEngine gives " + queryEngine.getIntegerIdOfSong(popularSongs[index]) + " and the file gives " + line);
      } else {
        logger.info("Rank#" + index + " Error! QueryEngine gives " + queryEngine.getIntegerIdOfSong(popularSongs[index]) + " and the file gives " + line);
        errorCount++;
      }
      index++;
    }
    return errorCount;
  }
  
  public int validatePopSongsWithListenedNum(String popSongFilePath) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File(popSongFilePath)));    
    logger.info("There are " + getNumOfPopSongsInFile(popSongFilePath) + " songs in the file.");
    String[] popularSongs = queryEngine.mostPopularSongs(getNumOfPopSongsInFile(popSongFilePath));
    int index = 0;
    int errorCount = 0;
    String line = null;
    while ((line = br.readLine()) != null) {
      int songId = Integer.valueOf(line.split("\\s+")[0]);
      int count  = Integer.valueOf(line.split("\\s+")[1]);
      if (queryEngine.getIntegerIdOfSong(popularSongs[index]) == songId) {
        logger.info("Rank#" + index + " Correct! QueryEngine gives " + queryEngine.getIntegerIdOfSong(popularSongs[index]) + " and the file gives " + songId);
        logger.info("Listened count from queryEngine: " + queryEngine.getListenersOf(queryEngine.getSongTagOfId(songId)).length + " and the listened count from file is: " + count); 
      } else {
        logger.info("Rank#" + index + " Error! QueryEngine gives " + queryEngine.getIntegerIdOfSong(popularSongs[index]) + " and the file gives " + songId);
        errorCount++;
      }
      index++;
    }
    return errorCount;
  }
  
  private int getNumOfPopSongsInFile(String filePath) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File("data/shared/popularSongs.txt")));
    int count = 0;
    while (br.readLine() != null) {
      count++;
    }
    return count;
  }
  
  public static void main(String[] args) throws IOException {
    ValidationUtil validateUtil = Guice.createInjector(new DistributedCFModule()).getInstance(ValidationUtil.class);
    int errorCount = validateUtil.validatePopSongs("data/shared/popularSongs.txt");
//    int errorCount = validateUtil.validatePopSongsWithListenedNum("data/shared/song_num_popularity_sorted.txt");
    System.out.println("Are they the same? " + (errorCount == 0 ? "Yes" : "No"));
    System.out.println("" + errorCount + " songs are different.");
  }
}
