package com.loading.nebula.lib;

import com.loading.nebula.config.Configs;
import com.loading.nebula.config.algo.LouvainConfigProxy;
import com.loading.nebula.exception.ConfigsIllegalException;
import com.vesoft.nebula.algorithm.lib.LouvainAlgo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/17
 */
public class LouvainAlgoHandler extends AbstractGraphAlgoHandler {

  public static final String ALGO_NAME = "Louvain";

  private LouvainConfigProxy algoConfig;

  public LouvainAlgoHandler(Configs configs) throws ConfigsIllegalException {
    super(configs);
    this.algoConfig = new LouvainConfigProxy(configs.getAlgoConfig().getConfig());
  }

  @Override
  protected void checkConfig(Configs configs) throws ConfigsIllegalException {

  }

  @Override
  protected Dataset<Row> executeAlgo() {
    return LouvainAlgo
        .apply(this.sparkSession, readEdges(), this.algoConfig, this.algoConfig.isHasWeight());
  }

  @Override
  public String getAlgoName() {
    return ALGO_NAME;
  }

  @Override
  protected Dataset<Row> afterExecuteAlgo(Dataset<Row> algoResult) {
    Dataset<Row> vData1 = readVertex();
    Dataset<Row> vData2 = readVertex();
    return algoResult
        .join(vData1,
            algoResult.col(Constants.ALGO_ID_COL).equalTo(vData1.col(Constants.V_LONG_ID)),
            "left")
        .join(vData2,
            algoResult.col(Constants.LOUVAIN_RESULT_COL).equalTo(vData2.col(Constants.V_LONG_ID)),
            "left");
  }

}
