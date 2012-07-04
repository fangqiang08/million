package org.zhengyang.kaggle.distributed;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.zhengyang.kaggle.inject.DistributedCFModule;
import org.zhengyang.kaggle.query.Query;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.internal.util.Lists;

/**
 * The master class will manage the following,
 * 0. Keep records of all computing tasks
 * 1. Put computing tasks from total working list into working queue
 * 2. Display message from message queue
 * 3. When there is nothing left in total working list, check if result queue has all results of all computing tasks
 * 
 * @author zhengyang.feng2011@gmail.com
 * @creation Jun 29, 2012
 */
public class Master {
  static Logger logger = Logger.getLogger(Master.class);
  
  private Query queryEngine;
  private JedisConnector jedisConnector;
  public long workQSize = 500;  
  public long interval = 2000;
  
  @Inject
  public Master(Query queryEngine, JedisConnector jedisConnector) {
    this.queryEngine = queryEngine;
    this.jedisConnector = jedisConnector;
  }
  
  public void setWorkQSize(int size) {
    workQSize = size;
  }
  
  public void setInterval(long interval) {
    this.interval = interval;
  }
  
  public void init() {
    // keep all tasks in TASK_RECORD_Q and ALL_TASK_Q
    logger.debug("Pushing " + queryEngine.allUsers().length + " tasks into TASK_RECORD_Q and REMAINING_TASK_Q");
    for (String uid : queryEngine.allUsers()) {
      jedisConnector.jedis().lpush(Constants.TASK_RECORD_Q, uid);
      jedisConnector.jedis().lpush(Constants.REMAINING_TASK_Q, uid);
    }
  }
  
  public void clearAllQueues() {
    logger.info("Clearing all queues.");
    jedisConnector.jedis().del(
        Constants.TASK_RECORD_Q, 
        Constants.REMAINING_TASK_Q, 
        Constants.WORKING_Q, 
        Constants.RESULT_KEY_Q, 
        Constants.MESSAGE_Q, 
        Constants.COMMAND_Q,
        Constants.RESULT_HASH);
    logger.info("All queues have been cleared.");
  }
  
  public void action() throws InterruptedException {
    // TODO : the master should check the result Q first, and decided what remained to be computing
    run();
    while (!validate()) {
      List<String> unhandledTasks = Lists.newArrayList();
      unhandledTasks.addAll(Arrays.asList(queryEngine.allUsers()));
      List<String> finishedTaskKeys = jedisConnector.jedis().lrange(Constants.RESULT_KEY_Q, 0, -1);
      unhandledTasks.removeAll(finishedTaskKeys);
      logger.info("There are " + unhandledTasks.size() + " task(s) failed, adding them back to REMAINING_WORK_Q");
      logger.info("Removing the failed tasks from RESULT_KEY_Q");
      // TODO : removing them from RESULT_KEY_Q
      jedisConnector.jedis().lpush(Constants.REMAINING_TASK_Q, unhandledTasks.toArray(new String[0]));
      run();
    }
    logger.info("Computation completed.");
  }
  
  public void run() throws InterruptedException {
    while (true) {
      // check if there is command, handle the command first
      String command = jedisConnector.jedis().lpop(Constants.COMMAND_Q);
      if (command != null) {
        logger.info("Received command:" + command);
        // TODO :
      }
      // check the size of the working q, if there is not enough tasks, add more
      long currentWorkingQSize = jedisConnector.jedis().llen(Constants.WORKING_Q);
      long remainingTaskQSize  = jedisConnector.jedis().llen(Constants.REMAINING_TASK_Q);
      if (currentWorkingQSize < workQSize && remainingTaskQSize > 0) {
        long numOfTaskAdded = remainingTaskQSize > workQSize ? workQSize : remainingTaskQSize;
        logger.info("Working queue size:" + currentWorkingQSize + ", will add " + numOfTaskAdded + " tasks to working queue.");
        List<String> tasks = Lists.newArrayList();
        for (int i = 0; i < numOfTaskAdded; i++) {
          tasks.add(jedisConnector.jedis().lpop(Constants.REMAINING_TASK_Q));
        }
        logger.info("Remaining task queue size: " + jedisConnector.jedis().llen(Constants.REMAINING_TASK_Q));
        jedisConnector.jedis().lpush(Constants.WORKING_Q, tasks.toArray(new String[0]));
        logger.info("Working queue size: " + jedisConnector.jedis().llen(Constants.WORKING_Q));
      }
      // if the remaining queue is empty, quit and perform validation
      if (jedisConnector.jedis().llen(Constants.REMAINING_TASK_Q) == 0 && jedisConnector.jedis().llen(Constants.WORKING_Q) == 0) {
        logger.info("Finished running, will perform validation.");
        break;
      }
      logger.info("Will rest for " + interval + " milliseconds. Remaining task: " + jedisConnector.jedis().llen(Constants.REMAINING_TASK_Q));
      Thread.sleep(interval);
    }  
  }
  
  protected boolean validate() {
    logger.info("Start validation of result...");
    long numOfResult = jedisConnector.jedis().llen(Constants.RESULT_KEY_Q);
    long numOfRecord = jedisConnector.jedis().llen(Constants.TASK_RECORD_Q);
    logger.info("Number of result is :" + numOfResult + ", and number of task records is: " + numOfRecord);
    if (numOfResult == numOfRecord) {
      logger.info("Validation passed!!");
    }   
    return numOfResult == numOfRecord;
  }
  
  public static void main(String[] args) throws InterruptedException {
    Master m = Guice.createInjector(new DistributedCFModule()).getInstance(Master.class);
    m.setWorkQSize(5);
    m.setInterval(2000);
    m.clearAllQueues();
    m.init();
    m.action();
  }
}
