package com.loading.nebula.config;

import com.typesafe.config.Config;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/19
 */
public class GraphConfig {

  private String metaAddress;

  private int connectionRetry = 2;

  private int connectionTimeOut = 600000;

  private GraphDataConfig graphDataConfig;

  public GraphConfig(){

  }

  public GraphConfig(Config config){
    this.metaAddress = config.getString("metaAddress");
    this.connectionRetry = config.getInt("connectionRetry");
    this.connectionTimeOut = config.getInt("connectionTimeOut");
    this.graphDataConfig = new GraphDataConfig(config.getConfig("graphDataConfig"));
  }

  public String getMetaAddress() {
    return metaAddress;
  }

  public void setMetaAddress(String metaAddress) {
    this.metaAddress = metaAddress;
  }

  public GraphDataConfig getGraphDataConfig() {
    return graphDataConfig;
  }

  public void setGraphDataConfig(GraphDataConfig graphDataConfig) {
    this.graphDataConfig = graphDataConfig;
  }

  public int getConnectionRetry() {
    return connectionRetry;
  }

  public void setConnectionRetry(int connectionRetry) {
    this.connectionRetry = connectionRetry;
  }

  public int getConnectionTimeOut() {
    return connectionTimeOut;
  }

  public void setConnectionTimeOut(int connectionTimeOut) {
    this.connectionTimeOut = connectionTimeOut;
  }
}
