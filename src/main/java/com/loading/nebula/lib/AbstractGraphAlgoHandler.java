package com.loading.nebula.lib;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.loading.nebula.config.Configs;
import com.loading.nebula.config.GraphDataConfig;
import com.loading.nebula.exception.ConfigsIllegalException;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.ReadNebulaConfig;
import com.vesoft.nebula.connector.connector.package$;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/13
 */
public abstract class AbstractGraphAlgoHandler {

  private static final int SEED = 0xc70f6907;

  protected NebulaConnectionConfig nebulaConnectionConfig = null;

  protected SparkSession sparkSession = null;

  protected Configs configs = null;

  public AbstractGraphAlgoHandler(Configs configs) throws ConfigsIllegalException {
    checkConfig(configs);
    this.configs = configs;
    buildNebulaConf();
    buildSparkSession();
    createUdf();
  }

  //TODO: 补充子类代码
  protected abstract void checkConfig(Configs configs) throws ConfigsIllegalException;

  /**
   * 执行算法
   *
   * @return Dataset<Row>
   */
  public Dataset<Row> execute() {
    return afterExecuteAlgo(executeAlgo());
  }

  protected abstract Dataset<Row> executeAlgo();

  protected Dataset<Row> afterExecuteAlgo(Dataset<Row> algoResult) {
    return algoResult;
  }

  /**
   * 构建nebula graph config
   */
  private void buildNebulaConf() {
    this.nebulaConnectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(this.configs.getGraphConfig().getMetaAddress())
        .withConenctionRetry(this.configs.getGraphConfig().getConnectionRetry())
        .withTimeout(this.configs.getGraphConfig().getConnectionTimeOut())
        .build();
  }

  /**
   * 构建spark session
   */
  private void buildSparkSession() {
    SparkConf sparkConf = new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    Class<?>[] classes = {TCompactProtocol.class};
    sparkConf.registerKryoClasses(classes);
    SparkContext sparkContext = new SparkContext(this.configs.getSparkConfig().getMaster(),
        "GraphAlgo-" + getAlgoName(),
        sparkConf);
    for (String jarPath : this.configs.getSparkConfig().getJars()) {
      sparkContext.addJar(jarPath);
    }
    this.sparkSession = new SparkSession(sparkContext);
  }

  /**
   * 构建计算long型id的udf函数
   */
  private void createUdf() {
    UDF1<String, Long> calculateLongId = (UDF1<String, Long>) value -> MurmurHash2
        .hash64(value.getBytes(), value.length(), SEED);
    this.sparkSession.udf()
        .register(Constants.UDF_NAME, calculateLongId, DataTypes.LongType);
  }

  /**
   * 读取边数据, 并将String类型id转为long型
   *
   * @return Dataset<Row> _srcId，_dstId，_rank
   */
  protected Dataset<Row> readEdges() {
    GraphDataConfig graphDataConfig = configs.getGraphConfig().getGraphDataConfig();
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(graphDataConfig.getSpace())
        .withLabel(graphDataConfig.getEdgeName())
        .withNoColumn(true)
        .withPartitionNum(graphDataConfig.getPartitionNum())
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(this.sparkSession);
    Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
        .nebula(this.nebulaConnectionConfig, readNebulaConfig).loadEdgesToDF();
    Dataset<Row> calculateLongIdDataset = dataset
        .withColumn(Constants.E_SRC_LONG_ID,
            functions.callUDF(Constants.UDF_NAME, dataset.col(Constants.E_SRC_ID)))
        .withColumn(Constants.E_DST_LONG_ID,
            functions.callUDF(Constants.UDF_NAME, dataset.col(Constants.E_DST_ID)));

    StructType structType2 = new StructType(new StructField[]{
        new StructField(Constants.E_SRC_ID, DataTypes.LongType, true, Metadata.empty()),
        new StructField(Constants.E_DST_ID, DataTypes.LongType, true, Metadata.empty()),
        new StructField(Constants.E_RANK, DataTypes.LongType, true, Metadata.empty())
    });
    return calculateLongIdDataset.map((MapFunction<Row, Row>) row -> RowFactory
            .create(row.getLong(row.fieldIndex(Constants.E_SRC_LONG_ID)),
                row.getLong(row.fieldIndex(Constants.E_DST_LONG_ID)),
                row.getLong(row.fieldIndex(Constants.E_RANK))),
        RowEncoder.apply(structType2));
  }

  /**
   * 读取点数据
   *
   * @return Dataset<Row> _vertexId，_vLid
   */
  protected Dataset<Row> readVertex() {
    GraphDataConfig graphDataConfig = configs.getGraphConfig().getGraphDataConfig();
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(graphDataConfig.getSpace())
        .withLabel(graphDataConfig.getVertexName())
        .withNoColumn(graphDataConfig.isVertexNoCol())
        .withReturnCols(
            JavaConversions.asScalaIterator(graphDataConfig.getVertexCols().iterator()).reversed())
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
    Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
        .nebula(nebulaConnectionConfig, readNebulaConfig).loadVerticesToDF();
    return dataset
        .withColumn(Constants.V_LONG_ID,
            functions.callUDF(Constants.UDF_NAME, dataset.col(Constants.V_ID)));
  }

  public String getAlgoName() {
    return "Algo";
  }

  public NebulaConnectionConfig getNebulaConnectionConfig() {
    return nebulaConnectionConfig;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

}
