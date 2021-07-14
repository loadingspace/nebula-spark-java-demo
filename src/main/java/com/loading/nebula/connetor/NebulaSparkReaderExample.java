package com.loading.nebula.connetor;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.ReadNebulaConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.immutable.List;
import com.vesoft.nebula.connector.connector.package$;
import scala.collection.immutable.List$;
import scala.collection.mutable.StringBuilder;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/4/9
 */
public class NebulaSparkReaderExample {

  private final Logger logger = LoggerFactory.getLogger(NebulaSparkWriterExample.class);

  private static final String SPARK_MASTER = "local";

  /**
   * update [META_ADDRESS] to your meta address
   */
  private static final String META_ADDRESS = "localhost:9559";

  private static final String SPACES = "test";

  private static final String TAG_NAME = "person";

  private static final String TAG_COL_1 = "name";

  private static final String TAG_COL_2 = "age";

  private static final String EDGE_NAME = "friend";

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    Class<?>[] classes = {TCompactProtocol.class};
    sparkConf.registerKryoClasses(classes);

    SparkContext sparkContext = new SparkContext(SPARK_MASTER, "NebulaSparkReaderExample",
        sparkConf);
    SparkSession sparkSession = new SparkSession(sparkContext);

    readData(sparkSession);

    sparkSession.close();
    System.exit(0);

  }

  private static void readData(SparkSession sparkSession) {

    // build connection config
    NebulaConnectionConfig nebulaConnectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(META_ADDRESS)
        .withConenctionRetry(2)
        .withTimeout(600000)
        .build();

    readVertex(sparkSession, nebulaConnectionConfig);
    readEdges(sparkSession, nebulaConnectionConfig);
    readVertexGraph(sparkSession, nebulaConnectionConfig);
    readEdgeGraph(sparkSession, nebulaConnectionConfig);
  }

  private static void readVertex(SparkSession sparkSession,
      NebulaConnectionConfig nebulaConnectionConfig) {

    List<String> cols = List$.MODULE$.empty();
    cols.addString(new StringBuilder(TAG_COL_1));
    cols.addString(new StringBuilder(TAG_COL_2));
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(SPACES)
        .withLabel(TAG_NAME)
        .withNoColumn(false)
        .withReturnCols(cols)
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
    Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
        .nebula(nebulaConnectionConfig, readNebulaConfig).loadVerticesToDF();
    System.out.println("Vertices schema");
    dataset.printSchema();
    dataset.show(20);
    System.out.println("Vertices nums:" + dataset.count());

  }

  private static void readEdges(SparkSession sparkSession,
      NebulaConnectionConfig nebulaConnectionConfig) {
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(SPACES)
        .withLabel(EDGE_NAME)
        .withNoColumn(true)
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
    Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
        .nebula(nebulaConnectionConfig, readNebulaConfig).loadEdgesToDF();
    System.out.println("Edge schema");
    dataset.printSchema();
    dataset.show(20);
    System.out.println("Edge nums:" + dataset.count());
  }

  private static void readVertexGraph(SparkSession sparkSession,
      NebulaConnectionConfig nebulaConnectionConfig) {
    List<String> cols = List$.MODULE$.empty();
    cols.addString(new StringBuilder("name"));
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(SPACES)
        .withLabel(TAG_NAME)
        .withReturnCols(cols)
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
    RDD<Tuple2<Object, List<Object>>> vertexRdd = package$.MODULE$
        .NebulaDataFrameReader(dataFrameReader).nebula(nebulaConnectionConfig, readNebulaConfig)
        .loadVerticesToGraphx();
    System.out.println("Vertices RDD nums:" + vertexRdd.count());
  }

  private static void readEdgeGraph(SparkSession sparkSession,
      NebulaConnectionConfig nebulaConnectionConfig) {
    ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(SPACES)
        .withLabel(EDGE_NAME)
        .withNoColumn(true)
        .build();
    DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
    RDD<Edge<Tuple2<Object, List<Object>>>> dataset = package$.MODULE$
        .NebulaDataFrameReader(dataFrameReader)
        .nebula(nebulaConnectionConfig, readNebulaConfig).loadEdgesToGraphx();
    System.out.println("Edge RDD nums:" + dataset.count());

  }

}
