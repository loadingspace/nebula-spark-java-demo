package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.LPAConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class LPAConfigProxy extends LPAConfig {

  private boolean hasWeight = false;

  public LPAConfigProxy() {
    super(5);
  }

  public LPAConfigProxy(Map<String, Object> config) {
    super(Integer.parseInt(config.getOrDefault("maxIter", "5").toString()));
    this.hasWeight = Boolean.parseBoolean(config.getOrDefault("hasWeight", "false").toString());
  }

  public LPAConfigProxy(int maxIter) {
    super(maxIter);
  }

  public boolean isHasWeight() {
    return hasWeight;
  }

  public void setHasWeight(boolean hasWeight) {
    this.hasWeight = hasWeight;
  }

}
