package org.zhengyang.kaggle.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;

public final class Utils {
  
  private Utils() {}
  
  /**
   * Return topN elements of the array, if the array doesn't have N elements, return the array itself.
   * @param arr
   * @param topN
   * @return String[] array
   */
  public static String[] getTopElementsOf(String[] arr, int topN) {
    return Arrays.copyOf(arr, min(arr.length, topN));
  }
  
  private static int min(int a, int b) {
    return a < b ? a : b;
  }

  /**
   * Sort the map by its value in descending order
   * @param songScoreMap
   * @return
   */
  public static <T extends Comparable<? super T>> String[] getSortedKeys(Map<String, T> songScoreMap) {
    List<String> sortedKeys = Lists.newArrayList();
    for (Map.Entry<String, T> entry : entriesSortedByValues(songScoreMap)) {
      sortedKeys.add(entry.getKey());
    }    
    return sortedKeys.toArray(new String[0]);
  }
  
  private static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(
      Map<K, V> map) {
    SortedSet<Map.Entry<K, V>> sortedEntries =
        new TreeSet<Map.Entry<K, V>>(new Comparator<Map.Entry<K, V>>() {
          public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
            int res = - e1.getValue().compareTo(e2.getValue());
            return res != 0 ? res : 1; // Special fix to preserve items with equal values
          }
        });
    sortedEntries.addAll(map.entrySet());
    return sortedEntries;
  }
}
