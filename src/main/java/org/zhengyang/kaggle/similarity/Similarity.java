package org.zhengyang.kaggle.similarity;

public interface Similarity {
  
  /**
   * 
   * @param song1
   * @param song2
   * @return the similarity of the two songs
   */
  double getSimilarity(String song1, String song2);
}
