package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.BetweennessConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class BetweennessConfigProxy extends BetweennessConfig {

  private boolean hasWeight = false;

  public BetweennessConfigProxy() {
    super(5);
  }

  public BetweennessConfigProxy(Map<String, Object> config) {
    super(Integer.parseInt(config.getOrDefault("maxIter", "5").toString()));
    this.hasWeight = Boolean.parseBoolean(config.getOrDefault("hasWeight", "false").toString());
  }

  public BetweennessConfigProxy(int maxIter) {
    super(maxIter);
  }

  public boolean isHasWeight() {
    return hasWeight;
  }

  public void setHasWeight(boolean hasWeight) {
    this.hasWeight = hasWeight;
  }
}
