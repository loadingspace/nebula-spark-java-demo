package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.PRConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class PRConfigProxy extends PRConfig {

  private boolean hasWeight = false;

  public PRConfigProxy() {
    super(5, 1.0);
  }

  public PRConfigProxy(Map<String, Object> config) {
    super(Integer.parseInt(config.getOrDefault("maxIter", "5").toString()),
        Double.parseDouble(config.getOrDefault("resetProb", "1.0").toString()));
    this.hasWeight = Boolean.parseBoolean(config.getOrDefault("hasWeight", "false").toString());
  }

  public PRConfigProxy(int maxIter, double resetProb) {
    super(maxIter, resetProb);
  }

  public boolean isHasWeight() {
    return hasWeight;
  }

  public void setHasWeight(boolean hasWeight) {
    this.hasWeight = hasWeight;
  }
}
