package org.zhengyang.kaggle.inject;

import java.io.IOException;

import org.zhengyang.kaggle.core.CollaborativeFilterFactory;
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
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class DefaultCollaborateFilteringModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Similarity.class).to(CountSimilarity.class).in(Singleton.class);
    bind(PredictionVal.class).to(WeightedSumPrediction.class).in(Singleton.class);
    bind(OutputFormatter.class).to(DefaultFileOutputFormatter.class).in(Singleton.class);
    install(new FactoryModuleBuilder().build(CollaborativeFilterFactory.class));
  }
  
  @Provides @Singleton
  Query providesLocalQueryEngine() throws IOException {
//    return new LocalQueryEngine("data/kaggle_visible_evaluation_triplets.txt");
    Query queryEngine = new LocalQueryEngine("data/small_triplets2.txt", "data/small_triplets2_colistened.txt", 500, "data/origin/kaggle_songs.txt");
    queryEngine.start();
    return queryEngine;
  }
  
  @Provides @Singleton
  JedisConnector providesJedisConnector() {
    JedisConnector jedisConnector = new JedisConnector("localhost", 6379);
    return jedisConnector;
  }
}
