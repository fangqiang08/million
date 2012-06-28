package org.zhengyang.kaggle.core;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.Assisted;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jun 28, 2012
 */
public interface CollaborativeFilterFactory {
  
  @Inject @Singleton
  CollaborativeFilter createCollaborativeFilter(
      @Assisted("numberOfPopSongs") int numberOfPopSongs,
      @Assisted("numberOfSongRecommended") int numberOfSongRecommended,
      @Assisted("outputPath") String outputPath); 
}
