package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.LouvainConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class LouvainConfigProxy extends LouvainConfig {

  private boolean hasWeight = false;

  public LouvainConfigProxy() {
    super(5, 10, 4.0);
  }

  public LouvainConfigProxy(Map<String, Object> config) {
    super(
        Integer.parseInt(config.getOrDefault("maxIter", "5").toString()),
        Integer.parseInt(config.getOrDefault("internallter", "3").toString()),
        Double.parseDouble(config.getOrDefault("tol", "4.0").toString()));
    this.hasWeight = Boolean.parseBoolean(config.getOrDefault("hasWeight", "false").toString());
  }

  public LouvainConfigProxy(int maxIter, int internalIter, double tol) {
    super(maxIter, internalIter, tol);
  }

  public boolean isHasWeight() {
    return hasWeight;
  }

  public void setHasWeight(boolean hasWeight) {
    this.hasWeight = hasWeight;
  }
}
