package com.loading.nebula;

import com.loading.nebula.config.Configs;
import com.loading.nebula.exception.NotFindAlgoException;
import com.loading.nebula.lib.BetweennessCentralityAlgoHandler;
import com.loading.nebula.lib.ConnectedComponentAlgoHandler;
import com.loading.nebula.lib.DegreeStaticAlgoHandler;
import com.loading.nebula.lib.KCoreAlgoHandler;
import com.loading.nebula.lib.LabelPropagationAlgoHandler;
import com.loading.nebula.lib.LouvainAlgoHandler;
import com.loading.nebula.lib.PageRankAlgoHandler;
import com.loading.nebula.lib.StronglyConnectedComponentAlgoHandler;
import com.loading.nebula.lib.TriangleCountAlgoHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class Main {

  public static void main(String[] args) throws Exception {
    String filepath = args[0];
    executeAlgorithm(buildConfigs(filepath));
    System.exit(0);
  }

  public static Configs buildConfigs(String filePath) {
    Config config = ConfigFactory.parseFile(new File(filePath));
    return new Configs(config);
  }

  public static void executeAlgorithm(Configs configs) throws Exception {
    String algoName = configs.getAlgoConfig().getAlgoName();
    switch (algoName) {
      case "pagerank":
        new PageRankAlgoHandler(configs).execute();
        break;
      case "louvain":
        new LouvainAlgoHandler(configs).execute();
        break;
      case "connectedcomponent":
        new ConnectedComponentAlgoHandler(configs).execute();
        break;
      case "labelpropagation":
        new LabelPropagationAlgoHandler(configs).execute();
        break;
      case "degreestatic":
        new DegreeStaticAlgoHandler(configs).execute();
        break;
      case "kcore":
        new KCoreAlgoHandler(configs).execute();
        break;
      case "stronglyconnectedcomponent":
        new StronglyConnectedComponentAlgoHandler(configs).execute();
        break;
      case "betweenness":
        new BetweennessCentralityAlgoHandler(configs).execute();
        break;
      case "trianglecount":
        new TriangleCountAlgoHandler(configs).execute();
        break;
      default:
        throw new NotFindAlgoException("unknown execute algo name. name is:[" + algoName + "]");
    }
  }

}
