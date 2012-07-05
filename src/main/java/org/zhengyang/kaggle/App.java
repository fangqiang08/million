package org.zhengyang.kaggle;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.DistributedCFModule;
import org.zhengyang.kaggle.utils.DistributedUtil;

import com.google.inject.Guice;
import com.google.inject.internal.util.Lists;

public class App {
  static Logger logger = Logger.getLogger(App.class);
  
  public static void main(String[] args) throws IOException, InterruptedException {
    DistributedUtil util = Guice.createInjector(new DistributedCFModule()).getInstance(DistributedUtil.class);  
    if (args.length == 0) {
      System.out.println("Please specify the parameters for this program.");
    }
    List<String> params = Lists.newArrayList(args);
    if (params.contains("ip")) {
      util.setIp(params.get(params.indexOf("-t") + 1));
    } else {
      util.setIp("127.0.0.1");
    }
    if (params.contains("worker")) {
      int workerNum = Integer.valueOf(params.get(params.indexOf("-t") + 1));
      int popSongNum = Integer.valueOf(params.get(params.indexOf("-p") + 1));
      util.startWorkers(util.hireWorkers(workerNum, popSongNum, 500));
    }
    if (params.contains("master")) {
      util.startMaster(util.createMaster(2000, 200));      
    }   
  }
}
