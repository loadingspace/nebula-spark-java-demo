package com.loading.nebula.lib;

import com.loading.nebula.config.AlgoConfig;
import com.loading.nebula.config.Configs;
import com.loading.nebula.config.GraphConfig;
import com.loading.nebula.config.GraphDataConfig;
import com.loading.nebula.config.SparkConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class AlgoHandlerTest {

  @Test
  public void executeBC() throws Exception {
    Dataset<Row> result = new BetweennessCentralityAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeDS() throws Exception {
    Dataset<Row> result = new DegreeStaticAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeKC() throws Exception {
    Dataset<Row> result = new KCoreAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeLP() throws Exception {
    Dataset<Row> result = new LabelPropagationAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeL() throws Exception {
    Dataset<Row> result = new LouvainAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executePR() throws Exception {
    Dataset<Row> result = new PageRankAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeSCC() throws Exception {
    Dataset<Row> result = new StronglyConnectedComponentAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  @Test
  public void executeTC() throws Exception {
    Dataset<Row> result = new TriangleCountAlgoHandler(buildConfigs()).execute();
    result.show(100);
  }

  public static Configs buildConfigs() {

    GraphDataConfig graphDataConfig = new GraphDataConfig();
    graphDataConfig.setSpace("test");
    graphDataConfig.setEdgeName("relation");
    graphDataConfig.setVertexName("user");
    graphDataConfig.setVertexNoCol(false);
    graphDataConfig.setVertexCols(new ArrayList<>(Collections.singletonList("name")));

    GraphConfig graphConfig = new GraphConfig();
    graphConfig.setGraphDataConfig(graphDataConfig);
    graphConfig.setMetaAddress("localhost:9559");

    SparkConfig sparkConfig = new SparkConfig();
    sparkConfig.setMaster("local");
//    sparkConfig.setJars(new String[]{"nebula-spark-java-demo-1.0-SNAPSHOT.jar"});

    AlgoConfig algoConfig = new AlgoConfig();
    Map<String, Object> properties = new HashMap<>();
    properties.put("hasWeight", true);
    algoConfig.setConfig(properties);

    Configs configs = new Configs();
    configs.setAlgoConfig(algoConfig);
    configs.setGraphConfig(graphConfig);
    configs.setSparkConfig(sparkConfig);

    return configs;

  }

}