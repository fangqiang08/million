package org.zhengyang.kaggle.query;

import java.io.IOException;


public interface Query {
  
  void start() throws IOException;
  boolean hasStarted();
  
  /**
   * @param userId
   * @return the list of songs that user has not listened
   */
  String[] hasNotListened(String userId);
  
  /**
   * 
   * @param number
   * @return the top N songs 
   */
  String[] mostPopularSongs(int number);
  
  /**
   * 
   * @param songId
   * @return the list of user id who has listened songId
   */
  String[] getListenersOf(String songId);
  
  /**
   * 
   * @return a list of all users' id
   */
  String[] allUsers();
  
  /**
   * 
   * @param userId
   * @return the listening history of a given user
   */
  String[] listenedSongs(String userId);
  
  /**
   * 
   * @param userId
   * @param songId
   * @return 
   */
  double getRating(String userId, String songId);
  
  String[] possibleRecommendation(String userId, int numberOfPopSongs);
  
  /**
   * Calculate the number of common audience of the two songs.
   * NOTE that this method will ONLY CALCULATE songs in co-listened matrix, if a song is not in co-listen matrix, its similarity to any song will be 0.
   * This is for computing efficiency.
   * 
   * @param song1
   * @param song2
   * @param numOfPopSongsCondidered
   * @return
   */
  long numberOfCommonAudience(String song1, String song2);
  
  int getIntegerIdOfSong(String hashTag);
  
  String getSongTagOfId(int id);
}
