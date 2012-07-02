package org.zhengyang.kaggle.similarity;

import java.util.Arrays;

import org.zhengyang.kaggle.query.Query;

import com.google.inject.Inject;

/**
 * Measure the similarity of two songs by how many users have listened both of the two songs.
 * @author Zhengyang
 *
 */
public class CountSimilarity implements Similarity {
  private Query queryEngine;
  private static double base = 5;
  
  @Inject
  public CountSimilarity(Query queryEngine) {
    this.queryEngine = queryEngine;
  }

  /**
   * TODO : Make the similarity range from 0 - 10
   */
  public double getSimilarity(String song1, String song2) {    
    return queryEngine.numberOfCommonAudience(song1, song2) / base;
  }

  protected boolean containedBoth(String[] arr, String str1, String str2) {
    return Arrays.asList(arr).contains(str1) && Arrays.asList(arr).contains(str2);
  }
}
