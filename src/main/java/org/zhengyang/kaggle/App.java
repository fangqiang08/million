package org.zhengyang.kaggle;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.distributed.Master;
import org.zhengyang.kaggle.distributed.Worker;
import org.zhengyang.kaggle.inject.JedisTestCFModule;

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
  
  private Master m;
  private List<Worker> workers = Lists.newArrayList();
  
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
  
  public List<Worker> hireWorkers(int numberOfWorkders) {
    List<Worker> workers = Lists.newArrayList();
    for (int i = 0; i < numberOfWorkders; i++) {
      workers.add(createWorker(numberOfPopSongs, numberOfSongRecommended));
    }
    return workers;
  }
  
  private Worker createWorker(int numberOfPopSongs, int numberOfSongRecommended) {
    Worker w = injector.getInstance(Worker.class);
    w.setNumberOfPopSongs(numberOfPopSongs);
    w.setNumberOfSongRecommended(numberOfSongRecommended);
    return w;
  }
  
  public void run() throws InterruptedException {
    m = injector.getInstance(Master.class);
    m.setWorkQSize(workQSize);
    m.setInterval(masterInterval);
    m.clearAllQueues();
    m.init();
    workers = hireWorkers(4);
    startWorkers(workers);
    m.action();
    logger.info("Stopping workers...");
    stopWorkers(workers);
  }
  
  private void startWorkers(List<Worker> workers) {
    for (Worker w : workers) {
      new Thread(w).start();     
    }
  }
 
  private void stopWorkers(List<Worker> workers) {
    for (Worker w : workers) {
      w.stop();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    // params : master interval, num of pop songs, num of songs to recommend
    new App(2000, 1000, 500).run();
  }
  
  public void setWorkQSize(int workQSize) {
    this.workQSize = workQSize;
  }

  public void setMasterInterval(int masterInterval) {
    this.masterInterval = masterInterval;
  }

  public void setNumberOfPopSongs(int numberOfPopSongs) {
    this.numberOfPopSongs = numberOfPopSongs;
  }

  public void setNumberOfSongRecommended(int numberOfSongRecommended) {
    this.numberOfSongRecommended = numberOfSongRecommended;
  }
}
