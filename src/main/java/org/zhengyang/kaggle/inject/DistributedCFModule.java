package org.zhengyang.kaggle.inject;

import java.io.IOException;

import org.zhengyang.kaggle.distributed.JedisConnector;
import org.zhengyang.kaggle.io.DefaultFileOutputFormatter;
import org.zhengyang.kaggle.io.OutputFormatter;
import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.prediction.WeightedSumPrediction;
import org.zhengyang.kaggle.query.LocalQueryEngine;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.similarity.CountSimilarity;
import org.zhengyang.kaggle.similarity.Similarity;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * @author zhengyang.feng2011@gmail.com (Zhengyang Feng)
 * @creation Jun 29, 2012
 */
public class DistributedCFModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(Similarity.class).to(CountSimilarity.class);
    bind(PredictionVal.class).to(WeightedSumPrediction.class);
    bind(OutputFormatter.class).to(DefaultFileOutputFormatter.class);
  }

  @Provides
  @Singleton
  Query providesLocalQueryEngine() throws IOException {
    Query queryEngine = new LocalQueryEngine(
        "data/kaggle_visible_evaluation_triplets.txt", 
        "data/origin/kaggle_songs.txt", 
        providesJedisConnector());
    queryEngine.start();
    return queryEngine;
  }

  @Provides
  JedisConnector providesJedisConnector() {
    JedisConnector jedisConnector = new JedisConnector("127.0.0.1", 6379);
    return jedisConnector;
  }
}
