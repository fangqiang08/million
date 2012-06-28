package org.zhengyang.kaggle.utils;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

public class UtilsTest {

  @Test
  public void testGetTopElementsOf() {
    String[] strArr = {"a", "b", "c"};
    assertEquals(1, Utils.getTopElementsOf(strArr, 1).length);
    assertEquals("a", Utils.getTopElementsOf(strArr, 1)[0]);
    
    assertEquals(3, Utils.getTopElementsOf(strArr, 3).length);
    assertEquals("a", Utils.getTopElementsOf(strArr, 3)[0]);
    assertEquals("b", Utils.getTopElementsOf(strArr, 3)[1]);
    assertEquals("c", Utils.getTopElementsOf(strArr, 3)[2]);
  }
  
  @Test
  public void testGetSortedKeys() {
    Map<String, Double> songScoreMap = Maps.newHashMap();
    songScoreMap.put("a", 1.2);
    songScoreMap.put("b", 99.0);
    songScoreMap.put("c", 0.0);
    songScoreMap.put("d", -10.0);
    String[] sortedKeys = Utils.getSortedKeys(songScoreMap);
    
    assertEquals(4, sortedKeys.length);
    assertEquals("b", sortedKeys[0]);
    assertEquals("a", sortedKeys[1]);
    assertEquals("c", sortedKeys[2]);
    assertEquals("d", sortedKeys[3]);
  }

}
