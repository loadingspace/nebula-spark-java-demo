package com.loading.nebula.config;

import com.typesafe.config.Config;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/19
 */
public class SparkConfig {

  private String master;

  private String[] jars = new String[]{};

  public SparkConfig() {

  }

  public SparkConfig(Config config) {
    this.master = config.getString("master");
  }

  public String getMaster() {
    return master;
  }

  public void setMaster(String master) {
    this.master = master;
  }

  public String[] getJars() {
    return jars;
  }

  public void setJars(String[] jars) {
    this.jars = jars;
  }
}
