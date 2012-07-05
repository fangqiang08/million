package org.zhengyang.kaggle.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.distributed.Constants;
import org.zhengyang.kaggle.distributed.JedisConnector;
import org.zhengyang.kaggle.inject.DistributedCFModule;
import org.zhengyang.kaggle.utils.DataUtil;
import org.zhengyang.kaggle.utils.Utils;

import redis.clients.jedis.Jedis;

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
  // maps <userId, songId> pair to the number of songs that has been listened by the user
  // the key is userId - songId
  protected Map<String, Integer>      userSongCountMap = Maps.newHashMap();
  // save all songs as a set in memory, so make hasNotListened() function faster
  protected Map<String, Integer>      songIndexMap     = Maps.newHashMap();
  protected Map<Integer, String>      indexSongMap     = Maps.newHashMap();
  // save all users in memory to make the query faster
  protected Set<String>               allUsers         = Sets.newHashSet();
  // save most popular songs in memory to make the query faster
  protected Map<String, Integer>      songsPopularityMap;
  protected String[]                  songsSortedByPopularity;
  
  
  /**
   * Special attention, the matrix only store non-zero elements. The key for this matrix is songId1 + songId2.
   */
  protected Map<String, Integer>  colistenedMap        = Maps.newHashMap();
  private boolean                     started          = false;
  private String trainingDataPath                      = null;
  private String songIndexFilePath                     = null;
  private JedisConnector jedis;
  private String colistenedFilePath;
  private String songsByPopularityFilePath;
  
  /**
   * 
   * @param tripletPath the userId, songId, song count file path
   * @param colistenedMatrixFilePath the place to save the colistened file
   * @param sizeOfColistenedMatrix 0 means all songs
   * @param songIndexFilePath
   */
  @Inject
  public LocalQueryEngine(
      String tripletPath, 
      String songIndexFilePath, 
      String colistenedFilePath,
      String songsByPopularityFilePath,
      JedisConnector jedis) {
    this.trainingDataPath          = tripletPath;
    this.songIndexFilePath         = songIndexFilePath;
    this.colistenedFilePath        = colistenedFilePath;
    this.songsByPopularityFilePath = songsByPopularityFilePath;
    this.jedis                     = jedis;
  }
  
  private Jedis jedis() {
    return jedis.jedis();
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
      userSongCountMap.put(userId + "-" + songId, count);
      // add user to allUsers set      
      allUsers.add(userId);     
    }  
    buildSongIndexMap(songIndexFilePath);
    loadSongsByPopularity(songsByPopularityFilePath);
    loadColistenedMap(colistenedFilePath);
    started = true;
    logger.info("Local query engine has started!");
    logger.info("Songs number: " + songsPopularityMap.size());
    logger.info("Users number: " + allUsers.size());
    sb.close();
  } 

  private void loadSongsByPopularity(String songsByPopularityFilePath) throws IOException {
    logger.info("Start loading songs' popularity file...");
    songsPopularityMap      = DataUtil.getSongsByPopularity(songsByPopularityFilePath);
    songsSortedByPopularity = Utils.getSortedKeys(songsPopularityMap);
  }

  private void loadColistenedMap(String colistenedFilePath) throws IOException {
    colistenedMap = DataUtil.loadColistenedFile(colistenedFilePath);
  }

  private void buildSongIndexMap(String songIndexFilePath) throws IOException {
    logger.info("Start building songIndexMap...");
    BufferedReader br = new BufferedReader(new FileReader(new File(songIndexFilePath)));
    String line = null;
    while ((line = br.readLine()) != null) {
      songIndexMap.put(line.split("\\s+")[0], Integer.valueOf(line.split("\\s+")[1]));
      indexSongMap.put(Integer.valueOf(line.split("\\s+")[1]), line.split("\\s+")[0]);
    }
    br.close();
  }

  public boolean hasStarted() {
    return started;
  }
  
  /**
   * Clear all buffers
   */
  public void close() {
    songUserMap        = Maps.newHashMap();
    userSongMap        = Maps.newHashMap();
    userSongCountMap   = Maps.newHashMap();
    songsPopularityMap = Maps.newHashMap();
    allUsers           = Sets.newHashSet();
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
    Set<String> temp = Sets.newHashSet(allSongs());
    temp.removeAll(userSongMap.get(userId));
    return temp.toArray(new String[0]);
  }

  /**
   * This method should be called only one time, since the result will be buffered.
   * So there is no need to optimize it.
   */
  public String[] mostPopularSongs(int numOfPopSongs) {
    return Arrays.copyOf(songsSortedByPopularity, numOfPopSongs);
  }

  public String[] getListenersOf(String songId) {
    return songUserMap.get(songId).toArray(new String[0]);
  }

  public String[] allUsers() {
    return allUsers.toArray(new String[0]);
  }
  
  public String[] allSongs() {
    return songsPopularityMap.keySet().toArray(new String[0]);
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
      int count = userSongCountMap.get(userId + "-" + songId);
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
    int song1id = getIntegerIdOfSong(song1);
    int song2id = getIntegerIdOfSong(song2);
    if (colistenedMap.containsKey(song1id + "-" + song2id)) {
      return colistenedMap.get(song1id + "-" + song2id);
    } else if (!colistenedMap.containsKey(song2id + "-" + song1id)) {
      return colistenedMap.get(song2id + "-" + song1id);
    }
    return 0;
  }
  
  public int getIntegerIdOfSong(String hashTag) {
    return songIndexMap.get(hashTag);
  }
  
  public String getSongTagOfId(int id) {
    return indexSongMap.get(id);
  }
  
  public static void main(String[] args) throws IOException {
    Query q = Guice.createInjector(new DistributedCFModule()).getInstance(Query.class);
  }
}
