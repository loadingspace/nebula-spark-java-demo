package com.loading.nebula.config;

import com.alibaba.fastjson.JSON;
import com.loading.nebula.Main;
import org.junit.Test;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class ConfigTest {

  @Test
  public void buildConfigs() {

    Configs configs = Main.buildConfigs("src/test/resources/application.conf");

    System.out.println(JSON.toJSONString(configs));

  }

}