package com.loading.nebula.config.algo;

import com.vesoft.nebula.algorithm.config.KCoreConfig;
import java.util.Map;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class KCoreConfigProxy extends KCoreConfig {

  public KCoreConfigProxy(Map<String, Object> config) {
    super(
        Integer.parseInt(config.getOrDefault("maxIter", "5").toString()),
        Integer.parseInt(config.getOrDefault("degree", "3").toString()));
  }

  public KCoreConfigProxy() {
    super(5, 3);
  }

  public KCoreConfigProxy(int maxIter, int degree) {
    super(maxIter, degree);
  }

}
