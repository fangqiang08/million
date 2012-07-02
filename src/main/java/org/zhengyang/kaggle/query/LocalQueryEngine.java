package org.zhengyang.kaggle.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.JedisTestCFModule;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.internal.util.Lists;

/**
 * The class simplely load all the data into memory as maps and sets.
 * @author Zhengyang
 *
 */
// TODO : add exceptions when cannot find a song/user by its id

public class LocalQueryEngine implements Query {
  static Logger logger = Logger.getLogger(LocalQueryEngine.class);
  
  protected Map<String, Set<String>> songUserMap      = Maps.newHashMap();
  protected Map<String, Set<String>> userSongMap      = Maps.newHashMap();
  // maps <userId, songId> pair to the number of songs that has been listened by
  // the user
  // the key is userId + songId
  protected Map<String, Integer>      userSongCountMap = Maps.newHashMap();
  // save all songs as a set in memory, so make hasNotListened() function faster
  protected Set<String>               allSongs         = Sets.newHashSet();
  protected Map<String, Integer>      songIndexMap     = Maps.newHashMap();
  protected Map<Integer, String>      indexSongMap     = Maps.newHashMap();
  // save all users in memory to make the query faster
  protected Set<String>               allUsers         = Sets.newHashSet();
  // save most popular songs in memory to make the query faster
  protected List<String>              mostPopular              = Lists.newArrayList();
  private int                         sizeOfColistenedMatrix   = 1000;
  /**
   * Special attention, the matrix only store non-zero elements. The key for this matrix is songId1 + songId2.
   */
  protected Map<String, Integer>  colistenedMap        = Maps.newHashMap();
  private boolean                     started          = false;
  private String trainingDataPath                      = null;
  private String colistenedMatrixFilePath              = null;
  private String songIndexFilePath                     = null;
  
  /**
   * 
   * @param trainingDataPath
   * @param colistenedMatrixFilePath
   * @param sizeOfColistenedMatrix 0 means all songs
   * @param songIndexFilePath
   */
  @Inject
  public LocalQueryEngine(String trainingDataPath, 
      String colistenedMatrixFilePath, 
      int sizeOfColistenedMatrix, 
      String songIndexFilePath) {
    this.trainingDataPath = trainingDataPath;
    this.colistenedMatrixFilePath = colistenedMatrixFilePath;
    this.sizeOfColistenedMatrix = sizeOfColistenedMatrix;
    this.songIndexFilePath = songIndexFilePath;
  }
  
  public void start() throws IOException {
    logger.info("Starting local query engine...");
    BufferedReader sb = new BufferedReader(new FileReader(trainingDataPath));
    String line = null;
    while ((line = sb.readLine()) != null) {
      String[] arr = line.split("\\s+");
      String userId = arr[0];
      String songId = arr[1];
      int    count  = Integer.valueOf(arr[2]);
      // add entry to songUserMap     
      addEntryToSongUserMap(userId, songId);
      // add entry to userSongMap    
      addEntryToUserSongMap(userId, songId);
      // add entry to userSongCountMap     
      userSongCountMap.put(userId + songId, count);
      // add song to the allSongs set 
      allSongs.add(songId);
      // add user to allUsers set      
      allUsers.add(userId);     
    }
    // build songIndexMap    
    logger.info("Building songIndexMap...");
    buildSongIndexMap(songIndexFilePath);
    // build song song matrix
    buildColistenedMap(colistenedMatrixFilePath);    
    started = true;
    logger.info("Local query engine started.");
    logger.info("Songs number: " + allSongs.size());
    logger.info("Users number: " + allUsers.size());
    sb.close();
  } 

  private void buildSongIndexMap(String songIndexFilePath) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File(songIndexFilePath)));
    String line = null;
    while ((line = br.readLine()) != null) {
      songIndexMap.put(line.split("\\s+")[0], Integer.valueOf(line.split("\\s+")[1]));
      indexSongMap.put(Integer.valueOf(line.split("\\s+")[1]), line.split("\\s+")[0]);
    }
    br.close();
  }

  /**
   * Because this is a REALLY TIME CONSUMING process, I need to save the result somewhere.
   * @param filePath
   * @throws IOException 
   */
  private void buildColistenedMap(String filePath) throws IOException {
    logger.info("Trying to find the co-listened matrix fils in " + "\"" + filePath + "\"");
    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
      String line = null;
      while ((line = br.readLine()) != null) {
        colistenedMap.put(line.split("\\s+")[0], Integer.valueOf(line.split("\\s+")[1]));
      }
      logger.info("Colistened matrix loaded successfully. Size: " + colistenedMap.size());
      return;
    } catch (FileNotFoundException e) {
      logger.info("The file for co-listened matrix cannot be found, will build a new one.");
    }
    logger.info("Start building the co-listened matrix...");
    colistenedMap = buildColistenedMap(sizeOfColistenedMatrix);
    logger.info("Finished building the co-listened matrix. Size: " + colistenedMap.size());
    logger.info("Saving co-listened matrix to file: \"" + filePath + "\"");
    saveColistenedMap(colistenedMap, filePath);
    logger.info("Saved.");
  }
  
  private void saveColistenedMap(Map<String, Integer> songSongMatrix, String filePath) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath)));
    for (String k : songSongMatrix.keySet()) {
      bw.write(k + " " + songSongMatrix.get(k) + "\n"); 
    }
    bw.close();
  }

  // TODO : It takes too long!!!
  // !!Solution!! Only calculate the similarity between most popular N songs!! For other songs.
  /**
   * This method is NOT thread-safe, it should be called only once.
   * @param size 0 means for all songs
   * @return
   */
  private Map<String, Integer> buildColistenedMap(int size) {
    Map<String, Integer> colistenedMatrix = Maps.newHashMap();
    long count = 0;
    long total = 0;
    String[] songs = null;
    // decide how many songs we need to consider
    if (size != 0) {
      total = mostPopularSongs(size).length;
      songs = mostPopularSongs(size);
    } else {
      total = allSongs().length;
      songs = allSongs();
    }
    for (int i = 0; i < songs.length; i++) {
      for (int j = 0; j < songs.length; j++) {
        if (i <= j) {
          continue;
        }
        List<String> song1Audience = Arrays.asList(getListenersOf(songs[i]));
        List<String> song2Audience = Arrays.asList(getListenersOf(songs[j]));
        List<String> common = Lists.newArrayList(song1Audience);
        common.retainAll(song2Audience);
        if (common.size() != 0) {
          colistenedMatrix.put(allSongs()[i] + "-" + allSongs()[j], common.size());
        }
      }
      count ++;
      if (count % 10 == 0) {
        logger.info("" + count +  "/" + total + " finished");
      }
    }
    return colistenedMatrix;
  }

//  private double roundTwoDecimals(double d) {
//    DecimalFormat twoDForm = new DecimalFormat("#.##");
//    return Double.valueOf(twoDForm.format(d));
//  }

  public boolean hasStarted() {
    return started;
  }
  
  /**
   * Clear all buffers
   */
  public void close() {
    songUserMap      = Maps.newHashMap();
    userSongMap      = Maps.newHashMap();
    userSongCountMap = Maps.newHashMap();
    allSongs         = Sets.newHashSet();
    allUsers         = Sets.newHashSet();
  }

  private void addEntryToUserSongMap(String userId, String songId) {
    if (userSongMap.containsKey(userId)) {
      userSongMap.get(userId).add(songId);
    } else {
      userSongMap.put(userId, new HashSet<String>());
      userSongMap.get(userId).add(songId);
    }
  }

  private void addEntryToSongUserMap(String userId, String songId) {    
    if (songUserMap.containsKey(songId)) {
      songUserMap.get(songId).add(userId);
    } else {
      songUserMap.put(songId, new HashSet<String>());
      songUserMap.get(songId).add(userId);
    }    
  }

  public String[] hasNotListened(String userId) {
    Set<String> temp = Sets.newHashSet(allSongs);
    temp.removeAll(userSongMap.get(userId));
    return temp.toArray(new String[0]);
  }

  /**
   * This method should be called only one time, since the result will be buffered.
   * So there is no need to optimize it.
   */
  public String[] mostPopularSongs(int numOfPopSongs) {
    if (mostPopular.size() >= numOfPopSongs) {
      return Arrays.copyOf(mostPopular.toArray(new String[0]), numOfPopSongs);
    }
    logger.info("Calculating top " + numOfPopSongs + " songs...");
    Map<String, Integer> songCountMap = Maps.newHashMap();
    for (String songId : allSongs) {
      songCountMap.put(songId, songUserMap.get(songId).size());
    }
    String[] sortedKeys = Utils.getSortedKeys(songCountMap);
    int length = sortedKeys.length < numOfPopSongs ? sortedKeys.length : numOfPopSongs;
    mostPopular = Arrays.asList(Arrays.copyOf(sortedKeys, length));
    logger.info("Finished calculating, get " + mostPopular.size() + " songs...");
    return mostPopular.toArray(new String[0]);
  }

  public String[] getListenersOf(String songId) {
    return songUserMap.get(songId).toArray(new String[0]);
  }

  public String[] allUsers() {
    return allUsers.toArray(new String[0]);
  }
  
  public String[] allSongs() {
    return allSongs.toArray(new String[0]);
  }

  public String[] listenedSongs(String userId) {
    return userSongMap.get(userId).toArray(new String[0]);
  }

  /**
   * 1. If user has not listened to this song, the rating is 0. 
   * 2. the more times user has listened to this song, the higher rating.
   */
  public double getRating(String userId, String songId) {
    // TODO : get a nice formula to calculate the rating of a song, and maybe make a separate class for this function
    if (userSongMap.get(userId).contains(songId)) {
      int count = userSongCountMap.get(userId + songId);
      return 1 + count * 0.1;
    }
    return 0.0;
  }
  
  /**
   * Possible recommendation: 1, popular songs 2, user has not listened
   * @param userId
   * @return array of possible recommended songs' id
   */
  public String[] possibleRecommendation(String userId, int numberOfPopSongs) {
    Set<String> popularSongs  = Sets.newHashSet(mostPopularSongs(numberOfPopSongs));    
    Set<String> listened      = Sets.newHashSet(listenedSongs(userId));
    popularSongs.removeAll(listened);
    return popularSongs.toArray(new String[0]);
  }

  public long numberOfCommonAudience(String song1, String song2) {
    if (colistenedMap.containsKey(song1 + "-" + song2)) {
      return colistenedMap.get(song1 + "-" + song2);
    } else if (colistenedMap.containsKey(song2 + "-" + song1)) {
      return colistenedMap.get(song2 + "-" + song1);
    } else {
      return 0;
    }    
  }
  
  public int getIntegerIdOfSong(String hashTag) {
    return songIndexMap.get(hashTag);
  }
  
  public String getSongTagOfId(int id) {
    return indexSongMap.get(id);
  }
  
  public static void main(String[] args) throws IOException {
    Query q = Guice.createInjector(new JedisTestCFModule()).getInstance(Query.class);
    System.out.println("Most popular song has " + q.getListenersOf(q.mostPopularSongs(1)[0]).length + " listeners.");
  }
}
