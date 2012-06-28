package org.zhengyang.kaggle.prediction;

public interface PredictionVal {
  
  /**
   * 
   * @param userId
   * @param songId
   * @return the prediction value of song to user, the higher the value is, it is more likely that use would love it
   */
  double getPredictionVal(String userId, String songId);
}
