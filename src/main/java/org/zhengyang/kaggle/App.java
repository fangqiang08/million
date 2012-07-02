package org.zhengyang.kaggle;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.JedisTestCFModule;
import org.zhengyang.kaggle.utils.DistributedUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.internal.util.Lists;

public class App {
  static Logger logger = Logger.getLogger(App.class);
  Injector injector = Guice.createInjector(new JedisTestCFModule());
  
  private int workQSize = 20;
  private int masterInterval = 2000;
  private int numberOfPopSongs = 600;
  private int numberOfSongRecommended = 500;

  /**
   * 
   * @param masterInterval the check interval of the master thread
   * @param numberOfPopSongs number of popular songs to check
   * @param numberOfSongRecommended number of songs to recommend
   */
  public App(int masterInterval, int numberOfPopSongs, int numberOfSongRecommended) {    
    this.masterInterval = masterInterval;
    this.numberOfPopSongs = numberOfPopSongs;
    this.numberOfSongRecommended = numberOfSongRecommended;
  }
  
  public void run() throws InterruptedException {
    DistributedUtil.startMaster(DistributedUtil.createMaster(masterInterval, workQSize));
    DistributedUtil.startWorkers(DistributedUtil.hireWorkers(4, numberOfPopSongs, numberOfSongRecommended));
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length == 0) {
      System.out.println("Please specify the parameters for this program.");
    }
    List<String> params = Lists.newArrayList(args);
    if (params.contains("worker")) {
      int workerNum = Integer.valueOf(params.get(params.indexOf("-t") + 1));
      int popSongNum = Integer.valueOf(params.get(params.indexOf("-p") + 1));
      DistributedUtil.startWorkers(DistributedUtil.hireWorkers(workerNum, popSongNum, 500));
    }
    if (params.contains("master")) {
      DistributedUtil.startMaster(DistributedUtil.createMaster(2000, 2000));      
    }   
  }
}
