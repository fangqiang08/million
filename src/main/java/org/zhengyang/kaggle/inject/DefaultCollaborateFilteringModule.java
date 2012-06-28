package org.zhengyang.kaggle.inject;

import org.zhengyang.kaggle.prediction.PredictionVal;
import org.zhengyang.kaggle.prediction.WeightedSumPrediction;
import org.zhengyang.kaggle.query.LocalQueryEngine;
import org.zhengyang.kaggle.query.Query;
import org.zhengyang.kaggle.similarity.CountSimilarity;
import org.zhengyang.kaggle.similarity.Similarity;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class DefaultCollaborateFilteringModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Similarity.class).to(CountSimilarity.class);
    bind(PredictionVal.class).to(WeightedSumPrediction.class);
  }
  
  @Provides
  Query providesLocalQueryEngine() {
    return new LocalQueryEngine("data/kaggle_visible_evaluation_triplets.txt");
  }
}
