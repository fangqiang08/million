package org.zhengyang.kaggle.query;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class LocalQueryEngineTest {
  
  String testFilePath = "data/tiny_triplets.txt";
  LocalQueryEngine localQueryEngine = new LocalQueryEngine(testFilePath);

  @Test
  public void testStart() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    assertEquals(4, localQueryEngine.songUserMap.size());
    assertEquals(2, localQueryEngine.songUserMap.get("song-1").size());
    assertEquals(4, localQueryEngine.userSongMap.size());
    assertEquals(2, localQueryEngine.userSongMap.get("user-tag-1").size());
    assertEquals(6, localQueryEngine.userSongCountMap.size());
    assertEquals(Integer.valueOf(1), localQueryEngine.userSongCountMap.get("user-tag-1song-1"));
    assertEquals(Integer.valueOf(2), localQueryEngine.userSongCountMap.get("user-tag-2song-3"));
    assertEquals(4, localQueryEngine.allSongs.size());
    assertEquals(4, localQueryEngine.allUsers.size());
  }

  @Test
  public void testHasNotListened() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testMostPopularSongs() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testGetListenersOf() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testGetRating() {
    fail("Not yet implemented");
  }
}
