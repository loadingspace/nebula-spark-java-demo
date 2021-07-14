package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.CcConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class CcConfigProxy extends CcConfig {

  private boolean hasWeight = false;

  public CcConfigProxy() {
    super(5);
  }

  public CcConfigProxy(Map<String, Object> config) {
    super(Integer.parseInt(config.getOrDefault("maxIter", "5").toString()));
    this.hasWeight = Boolean.parseBoolean(config.getOrDefault("hasWeight", "false").toString());
  }

  public CcConfigProxy(int maxIter) {
    super(maxIter);
  }

  public boolean isHasWeight() {
    return hasWeight;
  }

  public void setHasWeight(boolean hasWeight) {
    this.hasWeight = hasWeight;
  }
}
