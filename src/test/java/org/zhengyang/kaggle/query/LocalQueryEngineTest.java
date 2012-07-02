package org.zhengyang.kaggle.query;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class LocalQueryEngineTest {
  
  String testFilePath = "data/tiny_triplets.txt";
  String testColistenedMatrixFilePath = "data/tiny_triplets_colistened.txt";
  LocalQueryEngine localQueryEngine = new LocalQueryEngine(testFilePath, testColistenedMatrixFilePath, 5, "data/origin/kaggle_songs.txt");

  @Test
  public void testStart() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    assertEquals(4, localQueryEngine.songUserMap.size());
    assertEquals(3, localQueryEngine.songUserMap.get("song-1").size());
    assertEquals(5, localQueryEngine.userSongMap.size());
    assertEquals(2, localQueryEngine.userSongMap.get("user-tag-1").size());
    assertEquals(7, localQueryEngine.userSongCountMap.size());
    assertEquals(Integer.valueOf(1), localQueryEngine.userSongCountMap.get("user-tag-1song-1"));
    assertEquals(Integer.valueOf(2), localQueryEngine.userSongCountMap.get("user-tag-2song-3"));
    assertEquals(4, localQueryEngine.allSongs.size());
    assertEquals(5, localQueryEngine.allUsers.size());
  }

  @Test
  public void testHasNotListened() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    assertEquals(2, localQueryEngine.hasNotListened("user-tag-1").length);
    assertTrue(Arrays.asList(localQueryEngine.hasNotListened("user-tag-1")).contains("song-5"));
    assertTrue(Arrays.asList(localQueryEngine.hasNotListened("user-tag-1")).contains("song-3")); 
    assertEquals(2, localQueryEngine.hasNotListened("user-tag-2").length);
    assertTrue(Arrays.asList(localQueryEngine.hasNotListened("user-tag-2")).contains("song-1"));
    assertTrue(Arrays.asList(localQueryEngine.hasNotListened("user-tag-2")).contains("song-5"));  
  }
  
  @Test
  public void testMostPopularSongs() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    assertEquals(4, localQueryEngine.mostPopularSongs(10).length);
    assertEquals("song-1", localQueryEngine.mostPopularSongs(10)[0]);
    assertEquals("song-2", localQueryEngine.mostPopularSongs(10)[1]);
    assertTrue(Arrays.asList(localQueryEngine.mostPopularSongs(10)).contains("song-3"));
    assertTrue(Arrays.asList(localQueryEngine.mostPopularSongs(10)).contains("song-5"));
  }
  
  @Test
  public void testGetListenersOf() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    assertTrue(Arrays.asList(localQueryEngine.getListenersOf("song-1")).contains("user-tag-1"));
    assertTrue(Arrays.asList(localQueryEngine.getListenersOf("song-1")).contains("user-tag-3"));
    assertTrue(Arrays.asList(localQueryEngine.getListenersOf("song-1")).contains("user-tag-5"));
    assertEquals(3, localQueryEngine.getListenersOf("song-1").length);
    assertTrue(Arrays.asList(localQueryEngine.getListenersOf("song-5")).contains("user-tag-4"));
    assertEquals(1, localQueryEngine.getListenersOf("song-5").length);
  }
  
  @Test
  public void testGetRating() throws IOException {
    localQueryEngine.start();
    assertTrue(localQueryEngine.hasStarted());
    double tolerance = 0.001;
    assertEquals(2.5, localQueryEngine.getRating("user-tag-1", "song-1"), tolerance);
    assertEquals(4.0, localQueryEngine.getRating("user-tag-1", "song-2"), tolerance);
    assertEquals(0.0, localQueryEngine.getRating("user-tag-1", "song-3"), tolerance);
  }
}
