package org.zhengyang.kaggle.utils;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.distributed.Master;
import org.zhengyang.kaggle.distributed.Worker;
import org.zhengyang.kaggle.inject.DistributedCFModule;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.internal.util.Lists;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jul 2, 2012
 */
public class DistributedUtil {
  static Logger logger = Logger.getLogger(DistributedUtil.class);
  private Injector injector;
  
  @Inject
  public DistributedUtil() { }
  
  public void setIp(String ipAddress) {
    injector = Guice.createInjector(new DistributedCFModule(ipAddress));
  }
  
  public Master createMaster(int masterInterval, int workQSize) {
    Master m = injector.getInstance(Master.class);
    m.setWorkQSize(workQSize);
    m.setInterval(masterInterval);
    return m;
  }
  
  public void startMaster(Master m) throws InterruptedException, IOException {
    logger.info("Starting master thread, checking workQ interval: " + m.interval + " workQSize: " + m.workQSize);
    m.clearAllQueues();
    m.init();
    m.action();
  }
  
  public List<Worker> hireWorkers(int numberOfWorkders, int numberOfPopSongs, int numberOfSongRecommended) {
    List<Worker> workers = Lists.newArrayList();
    for (int i = 0; i < numberOfWorkders; i++) {
      workers.add(createWorker(numberOfPopSongs, numberOfSongRecommended));
    }
    return workers;
  }
  
  public void startWorkers(List<Worker> workers) {
    for (Worker w : workers) {
      logger.info("Starting worker thread, number of pop songs: " + w.numberOfPopSongs + " num of songs recommended: " + w.numberOfSongRecommended);
      new Thread(w).start();     
    }
  }
  
  private Worker createWorker(int numberOfPopSongs, int numberOfSongRecommended) {
    Worker w = injector.getInstance(Worker.class);
    w.setNumberOfPopSongs(numberOfPopSongs);
    w.setNumberOfSongRecommended(numberOfSongRecommended);
    return w;
  }
}
