package org.zhengyang.kaggle;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.core.CollaborativeFilter;
import org.zhengyang.kaggle.core.CollaborativeFilterFactory;
import org.zhengyang.kaggle.inject.DefaultCollaborateFilteringModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class App {
  static Logger logger = Logger.getLogger(App.class);
  Injector injector = Guice.createInjector(new DefaultCollaborateFilteringModule());
  private CollaborativeFilter cf;
  
  public App(String resultPath, int numberOfPopSongs, int numberOfSongRecommended) { 
    cf = 
        injector.getInstance(CollaborativeFilterFactory.class)
        .createCollaborativeFilter(numberOfPopSongs, numberOfSongRecommended, resultPath);
  }
  
  public void run() throws IOException {
    cf.run();
  }
  
  public static void main(String[] args) throws IOException {
    logger.info("Entering application.");
    new App("result/result-temp.txt", 1000, 500).run();
    logger.info("Exiting application.");
  }
}
