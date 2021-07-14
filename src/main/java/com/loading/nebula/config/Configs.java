package com.loading.nebula.config;

import com.typesafe.config.Config;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class Configs {

  private GraphConfig graphConfig;

  private SparkConfig sparkConfig;

  private AlgoConfig algoConfig;

  public Configs(){

  }

  public Configs(Config config){
    this.graphConfig = new GraphConfig(config.getConfig("graphConfig"));
    this.sparkConfig = new SparkConfig(config.getConfig("sparkConfig"));
    this.algoConfig = new AlgoConfig(config.getConfig("algoConfig"));
  }

  public GraphConfig getGraphConfig() {
    return graphConfig;
  }

  public void setGraphConfig(GraphConfig graphConfig) {
    this.graphConfig = graphConfig;
  }

  public SparkConfig getSparkConfig() {
    return sparkConfig;
  }

  public void setSparkConfig(SparkConfig sparkConfig) {
    this.sparkConfig = sparkConfig;
  }

  public AlgoConfig getAlgoConfig() {
    return algoConfig;
  }

  public void setAlgoConfig(AlgoConfig algoConfig) {
    this.algoConfig = algoConfig;
  }
}
