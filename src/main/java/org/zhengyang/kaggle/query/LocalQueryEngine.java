package org.zhengyang.kaggle.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.utils.Utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
  
  protected Map<String, List<String>> songUserMap      = Maps.newHashMap();
  protected Map<String, List<String>> userSongMap      = Maps.newHashMap();
  // maps <userId, songId> pair to the number of songs that has been listened by
  // the user
  // the key is userId + songId
  protected Map<String, Integer>      userSongCountMap = Maps.newHashMap();
  // save all songs as a set in memory, so make hasNotListened() function faster
  protected Set<String>               allSongs         = Sets.newHashSet();
  // save all users in memory to make the query faster
  protected Set<String>               allUsers         = Sets.newHashSet();
  // save most popular songs in memory to make the query faster
  protected List<String>              mostPopular      = Lists.newArrayList();
  protected Map<String, Integer> songSongMatrix        = Maps.newHashMap();
  private boolean                     started          = false;
  private String trainingDataPath                      = null;

  @Inject
  public LocalQueryEngine(String trainingDataPath) {
    this.trainingDataPath = trainingDataPath;
  }
  
  public void start() throws IOException {
    logger.info("Starting local query engine.");
    BufferedReader sb = new BufferedReader(new FileReader(trainingDataPath));
    String line = null;
    while ((line = sb.readLine()) != null) {
      String[] arr = line.split("\\s+");
      String userId = arr[0];
      String songId = arr[1];
      int count = Integer.valueOf(arr[2]);
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
      // build song song matrix
      buildSongSongMatrix();
    }
    started = true;
    logger.info("Local query engine started.");
    logger.info("Songs number: " + allSongs.size());
    logger.info("Users number: " + allUsers.size());
    sb.close();
  }
  
  private void buildSongSongMatrix() {
    // TODO : figure out a way to save it
  }

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
      userSongMap.put(userId, new ArrayList<String>());
      userSongMap.get(userId).add(songId);
    }
  }

  private void addEntryToSongUserMap(String userId, String songId) {    
    if (songUserMap.containsKey(songId)) {
      songUserMap.get(songId).add(userId);
    } else {
      songUserMap.put(songId, new ArrayList<String>());
      songUserMap.get(songId).add(userId);
    }    
  }

  public String[] hasNotListened(String userId) {
    Set<String> temp = Sets.newHashSet(allSongs);
    temp.removeAll(userSongMap.get(userId));
    return temp.toArray(new String[0]);
  }

  public String[] mostPopularSongs(int number) {
    if (mostPopular.size() == number) {
      return mostPopular.toArray(new String[0]);
    }
    Map<String, Integer> songCountMap = Maps.newHashMap();
    for (String songId : allSongs) {
      songCountMap.put(songId, songUserMap.get(songId).size());
    }
    String[] sortedKeys = Utils.getSortedKeys(songCountMap);
    int length = sortedKeys.length < number ? sortedKeys.length : number;
    mostPopular = Arrays.asList(Arrays.copyOf(Utils.getSortedKeys(songCountMap), length));
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
   * 1. If user has not listened to this song, the rate is 0. 
   * 2. the more times user has listened to this song, the higher rating.
   */
  public double getRating(String userId, String songId) {
    // TODO : get a nice formula to calculate the rating of a song, and maybe make a separate class for this function
    if (userSongMap.get(userId).contains(songId)) {
      int count = userSongCountMap.get(userId + songId);
      return 1 + count * 1.5;
    }
    return 0.0;
  }
  
  /**
   * Possible recommendation: 1, popular songs 2, user has not listened
   * @param userId
   * @return array of possible recommended songs' id
   */
  public String[] possibleRecommendation(String userId, int numberOfPopSongs) {
    Set<String> popularSongs = Sets.newHashSet(mostPopularSongs(numberOfPopSongs));    
    Set<String> listened      = Sets.newHashSet(listenedSongs(userId));
    popularSongs.removeAll(listened);
    return popularSongs.toArray(new String[0]);
  }

  public long numberOfCommonAudience(String song1, String song2) {
    // TODO : build the song-song matrix
    return 0;
  }
}
