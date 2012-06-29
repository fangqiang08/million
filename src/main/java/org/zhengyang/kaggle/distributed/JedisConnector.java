package org.zhengyang.kaggle.distributed;

import redis.clients.jedis.Jedis;

import com.google.inject.Inject;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jun 29, 2012
 */
public class JedisConnector {
  private Jedis jedis;
  
  @Inject
  public JedisConnector(String ip, int port) {
    jedis = new Jedis(ip, port);
  }
  
  public void disconnect() {
    jedis.disconnect();
  }
  
  public Jedis jedis() {
    return jedis;
  }
}
