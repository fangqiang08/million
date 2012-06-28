package org.zhengyang.kaggle.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public final class Utils {
  
  private Utils() {}
  
  public static String[] getTopElementsOf(String[] arr, int topN) {
    return Arrays.copyOf(arr, topN);
  }

  /**
   * Sort the map by its value in descending order
   * @param songScoreMap
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static <T extends Comparable> String[] getSortedKeys(Map<String, T> songScoreMap) {
    SortedMap<String, T> sortedMap = new TreeMap<String, T>(new ValueComparator<String, T>(songScoreMap));
    sortedMap.putAll(songScoreMap);
    return sortedMap.keySet().toArray(new String[0]);
  }
}

@SuppressWarnings("rawtypes")
class ValueComparator <T, V extends Comparable> implements Comparator<T> {
  Map<T, V> base;

  public ValueComparator(Map<T, V> base) {
    this.base = base;
  }

  @SuppressWarnings("unchecked")
  public int compare(T key1, T key2) {
    return -base.get(key1).compareTo(base.get(key2));
  }
}
