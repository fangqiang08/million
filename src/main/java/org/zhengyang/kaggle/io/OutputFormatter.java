package org.zhengyang.kaggle.io;

import java.io.IOException;
import java.util.Map;

/**
 * @author zhengyang.feng2011@gmail.com
 * @creation Jun 28, 2012
 */
public interface OutputFormatter {  
  void write(Map<String, String[]> userRecommendation, String[] userIds) throws IOException;
  void writeLine(String[] recommendation) throws IOException;
  void close() throws IOException;
  void setOutputPath(String outputPath) throws IOException;
}
