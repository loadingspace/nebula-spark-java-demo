package com.loading.nebula.lib;

import com.loading.nebula.config.Configs;
import com.loading.nebula.config.algo.CcConfigProxy;
import com.loading.nebula.exception.ConfigsIllegalException;
import com.vesoft.nebula.algorithm.lib.ConnectedComponentsAlgo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/18
 */
public class ConnectedComponentAlgoHandler extends AbstractGraphAlgoHandler {

  public static final String ALGO_NAME = "ConnectedComponent";

  private CcConfigProxy algoConfig;

  public ConnectedComponentAlgoHandler(Configs configs) throws ConfigsIllegalException {
    super(configs);
    this.algoConfig = new CcConfigProxy(configs.getAlgoConfig().getConfig());
  }

  @Override
  protected void checkConfig(Configs configs) throws ConfigsIllegalException {

  }

  @Override
  protected Dataset<Row> executeAlgo() {
    return ConnectedComponentsAlgo
        .apply(this.sparkSession, readEdges(), this.algoConfig, this.algoConfig.isHasWeight());
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
            algoResult.col(Constants.CC_RESULT_COL).equalTo(vData2.col(Constants.V_LONG_ID)),
            "left");
  }

  @Override
  public String getAlgoName() {
    return ALGO_NAME;
  }

}
