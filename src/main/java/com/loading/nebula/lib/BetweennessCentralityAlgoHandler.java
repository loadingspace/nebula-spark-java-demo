package com.loading.nebula.lib;

import com.loading.nebula.config.Configs;
import com.loading.nebula.config.algo.BetweennessConfigProxy;
import com.loading.nebula.exception.ConfigsIllegalException;
import com.vesoft.nebula.algorithm.lib.BetweennessCentralityAlgo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/18
 */
public class BetweennessCentralityAlgoHandler extends AbstractGraphAlgoHandler {

  public static final String ALGO_NAME = "BetweennessCentrality";

  private BetweennessConfigProxy algoConfig;

  public BetweennessCentralityAlgoHandler(Configs configs) throws ConfigsIllegalException {
    super(configs);
    this.algoConfig = new BetweennessConfigProxy(configs.getAlgoConfig().getConfig());
  }

  @Override
  protected void checkConfig(Configs configs) throws ConfigsIllegalException {
  }

  @Override
  protected Dataset<Row> executeAlgo() {
    return BetweennessCentralityAlgo
        .apply(this.sparkSession, readEdges(), this.algoConfig, this.algoConfig.isHasWeight());
  }

  @Override
  protected Dataset<Row> afterExecuteAlgo(Dataset<Row> algoResult) {
    Dataset<Row> vData = readVertex();
    return algoResult
        .join(vData, algoResult.col(Constants.ALGO_ID_COL).equalTo(vData.col(Constants.V_LONG_ID)),
            "left")
        .sort(algoResult.col(Constants.BETWEENNESS_RESULT_COL).desc());
  }

  @Override
  public String getAlgoName() {
    return ALGO_NAME;
  }


}
