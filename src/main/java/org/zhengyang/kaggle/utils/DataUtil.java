package org.zhengyang.kaggle.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.inject.internal.util.Maps;

public final class DataUtil {
  static Logger logger = Logger.getLogger(DataUtil.class);
  
  private DataUtil() {
  }
  
  public static Map<String, Integer> getSongsByPopularity(String dataFilePath) throws IOException {
    Map<String, Integer> songPopularityMap = Maps.newHashMap();
    BufferedReader br = new BufferedReader(new FileReader(new File(dataFilePath)));
    String line = null;
    while((line = br.readLine()) != null) {
      String[] parsed = line.split("\\s+");
      String key = parsed[0];
      Integer popularity = Integer.valueOf(parsed[1]);
      songPopularityMap.put(key, popularity);
    }
    br.close();
    return songPopularityMap;
  }
  
  public static void findMostPopularSongs(String inputTripletFilePath, String songsSortedByPopularityFilePath) throws IOException {
    Map<String, Integer> songPopularityMap = Maps.newHashMap();
    BufferedReader br = new BufferedReader(new FileReader(new File(inputTripletFilePath)));
    String line = null;
    int counter = 0;
    while((line = br.readLine()) != null) {
      counter ++;
      String[] parsed = line.split("\\s+");
      String songId = parsed[1];
      if (songPopularityMap.containsKey(songId)) {
        songPopularityMap.put(songId, songPopularityMap.get(songId) + 1);
      } else {
        songPopularityMap.put(songId, 1);
      }
      if (counter % 10000 == 0) {
        logger.info("finished reading line number: " + counter / 10000 + " * 10k");
      }     
    }
    br.close();
    logger.info("File has been loaded!");
    counter = 0;
    logger.info("sorting on values...");
    String[] mostPopularity = Utils.getSortedKeys(songPopularityMap);
    logger.info("finished sorting..");
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(songsSortedByPopularityFilePath)));
    for (String songId : mostPopularity) {
      counter ++;
      bw.write(songId + " " + songPopularityMap.get(songId) + "\n");
      if (counter % 10000 == 0) {
        logger.info("finished reading line number: " + counter);
      }  
    }
    logger.info("Finished writing!");
    bw.close();
  }
  
  public Map<String, Integer> loadPopularSongs(String popularSongPath) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File(popularSongPath)));
    String line = null;
    int counter = 0;
    Map<String, Integer> songPopularityMap = Maps.newHashMap();
    while((line = br.readLine()) != null) {
      counter ++;
      String[] parsed = line.split("\\s+");
      String songId = parsed[0];
      Integer popularity = Integer.valueOf(parsed[1]);
      songPopularityMap.put(songId, popularity);
      if (counter % 10000 == 0) {
        logger.info("finished reading line number: " + counter / 1000 + "k");
      }     
    }
    br.close();
    return songPopularityMap;
  }
  
  public static Map<String, Integer> loadColistenedFile(String colistenedFilePath) throws IOException {
    logger.info("Start loading the colisten matrix...");
    BufferedReader br = new BufferedReader(new FileReader(new File(colistenedFilePath)));
    String line = null;
    int counter = 0;
    Map<String, Integer> colistenedMap = Maps.newHashMap();
    while((line = br.readLine()) != null) {
      counter ++;
      String[] parsed = line.split("\\s+");
      String song1 = parsed[0];
      String song2 = parsed[1];
      Integer colistened = Integer.valueOf(parsed[2]);
      if (!colistenedMap.containsKey(song2 + "-" + song1)) {
        colistenedMap.put(song1 + "-" + song2, colistened);
      }
//      if (counter % 100000 == 0) {
//        logger.info("finished reading line number: " + counter / 100000 + " * 100k");
//      }     
    }
    br.close();
    logger.info("Finished loading the colisten matrix...");
    return colistenedMap;
  }
  
  public static void main(String[] args) throws IOException {
    long startTime = System.currentTimeMillis();
    // DataUtil.findMostPopularSongs("data/origin/train_triplets.txt", "data/derived/pop_songs_from_train.txt");
    DataUtil.loadColistenedFile("data/shared/colisten-matrix.txt");
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    String timeUsed = String.format("%d min, %d sec", 
        TimeUnit.MILLISECONDS.toMinutes(duration),
        TimeUnit.MILLISECONDS.toSeconds(duration) - 
        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration))
    );
    logger.info("Total time used: " + timeUsed);
  }
}
