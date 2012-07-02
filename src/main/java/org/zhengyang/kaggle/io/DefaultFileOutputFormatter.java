package org.zhengyang.kaggle.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.inject.Inject;

/**
 * @author zhengyang.feng2011@gmail.com (Zhengyang Feng)
 * @creation Jun 28, 2012
 */
public class DefaultFileOutputFormatter implements OutputFormatter {
  static Logger logger = Logger.getLogger(DefaultFileOutputFormatter.class);
  private BufferedWriter out;
  private Joiner joiner = Joiner.on(" ").skipNulls();
  
  @Inject
  public DefaultFileOutputFormatter() {   
  }
  
  public void write(Map<String, String[]> userRecommendation, String[] userIds) throws IOException {            
    for (String userId : userIds) {
      String line = joiner.join(userRecommendation.get(userId));
      out.write(line + "\n");
    }
  }

  public void writeLine(String[] recommendation) throws IOException {
    out.write(joiner.join(recommendation) + "\n");
    out.flush();
  }
  
  public String formatRecommendation(String[] recommendation) {
    logger.debug("Formattring " + recommendation.length + " songs to one line ----");
    String formatted = joiner.join(recommendation);
    logger.debug(formatted);
    return formatted;
  }

  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }

  public void setOutputPath(String outputPath) throws IOException {
    out = new BufferedWriter(new FileWriter(outputPath));
  }
}
