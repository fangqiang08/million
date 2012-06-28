package org.zhengyang.kaggle.similarity;

import java.util.Arrays;
import java.util.Map;

import org.zhengyang.kaggle.query.Query;

import com.google.common.collect.Maps;
import com.google.inject.Inject;

/**
 * Measure the similarity of two songs by how many users have listened both of the two songs.
 * @author Zhengyang
 *
 */
public class CountSimilarity implements Similarity {
  private Query queryEngine;
  
  // Buffer the calculation result, the key is song1Id + song2Id
  private Map<String, Integer> similaritySongMap = Maps.newHashMap();
  
  @Inject
  public CountSimilarity(Query queryEngine) {
    this.queryEngine = queryEngine;
  }

  public double getSimilarity(String song1, String song2) {    
    if (!similaritySongMap.containsKey(song1 + song2)) {
      int count = 0;
      for (String userId : queryEngine.allUsers()) {
        if (containedBoth(queryEngine.listenedSongs(userId), song1, song2)) {
          count++;
        }
      }
      similaritySongMap.put(song1 + song2, count);
      return count;
    }
    return similaritySongMap.get(song1 + song2);
  }

  protected boolean containedBoth(String[] arr, String str1, String str2) {
    return Arrays.asList(arr).contains(str1) && Arrays.asList(arr).contains(str2);
  }
}
