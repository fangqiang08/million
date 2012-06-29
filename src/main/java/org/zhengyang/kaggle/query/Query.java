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
}
