package com.loading.nebula.config;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class AlgoConfig {

  private String algoName;

  private Map<String, Object> config;

  public AlgoConfig(){

  }

  public AlgoConfig(Config config){
    this.algoName = config.getString("algoName");
    Map<String, Object> convert  = new HashMap<>();
    config.getConfig("config").entrySet().forEach(item -> {
      convert.put(item.getKey(), item.getValue().render());
    });
    this.config = convert;
  }

  public String getAlgoName() {
    return algoName;
  }

  public void setAlgoName(String algoName) {
    this.algoName = algoName;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

}