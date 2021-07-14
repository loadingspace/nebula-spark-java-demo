package com.loading.nebula.connetor;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.WriteNebulaEdgeConfig;
import com.vesoft.nebula.connector.WriteNebulaVertexConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import com.vesoft.nebula.connector.connector.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/4/9
 */
public class NebulaSparkWriterExample {

  private final Logger logger = LoggerFactory.getLogger(NebulaSparkWriterExample.class);

  private static final String SPARK_MASTER = "local";

  /**
   * update [META_ADDRESS] to your meta address
   */
  private static final String META_ADDRESS = "localhost:9559";

  /**
   * update [GRAPH_ADDRESS] to your graph address
   */
  private static final String GRAPH_ADDRESS = "localhost:9669";

  private static final String SPACES = "test";

  private static final String TAG_DATA_FILE = "/data/vertex";

  private static final String TAG_NAME = "person";

  private static final String TAG_VID_FIELD = "id";

  private static final String EDGE_DATA_FILE = "/data/edge";

  private static final String EDGE_NAME = "friend";

  private static final String EDGE_SRC = "src";

  private static final String EDGE_DST = "dst";

  private static final String EDGE_RANK = "degree";

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    Class<?>[] classes = {TCompactProtocol.class};
    sparkConf.registerKryoClasses(classes);

    SparkContext sparkContext = new SparkContext(SPARK_MASTER, "NebulaSparkWriterExample",
        sparkConf);
    SparkSession sparkSession = new SparkSession(sparkContext);

    writeData(sparkSession);

    sparkSession.close();
    System.exit(0);

  }

  private static void writeData(SparkSession sparkSession) {

    // build connection config
    NebulaConnectionConfig nebulaConnectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(META_ADDRESS)
        .withGraphAddress(GRAPH_ADDRESS)
        .withConenctionRetry(2)
        .build();

    //写入vertex数据
    System.out.println("Start to write nebula data [vertex]");
    Dataset<Row> vertexDataset = sparkSession.read().json(TAG_DATA_FILE);
    vertexDataset.show();
    vertexDataset.persist(StorageLevel.MEMORY_ONLY_SER());
    WriteNebulaVertexConfig writeNebulaVertexConfig = WriteNebulaVertexConfig
        .builder()
        .withSpace(SPACES)
        .withTag(TAG_NAME)
        .withVidField(TAG_VID_FIELD)
        .withVidAsProp(true)
        .withBatch(1000)
        .build();
    DataFrameWriter<Row> vertexDataFrameWriter = new DataFrameWriter<>(vertexDataset);
    package$.MODULE$.NebulaDataFrameWriter(vertexDataFrameWriter)
        .nebula(nebulaConnectionConfig, writeNebulaVertexConfig).writeVertices();
    System.out.println("End to write nebula data [vertex]");

    //写入edge数据
    System.out.println("Start to write nebula data [edge]");
    Dataset<Row> edgeDataset = sparkSession.read().json(EDGE_DATA_FILE);
    edgeDataset.show();
    edgeDataset.persist(StorageLevel.MEMORY_ONLY_SER());
    WriteNebulaEdgeConfig writeNebulaEdgeConfig = WriteNebulaEdgeConfig
        .builder()
        .withSpace(SPACES)
        .withEdge(EDGE_NAME)
        .withSrcIdField(EDGE_SRC)
        .withDstIdField(EDGE_DST)
        .withRankField(EDGE_RANK)
        .withSrcAsProperty(true)
        .withDstAsProperty(true)
        .withRankAsProperty(true)
        .withBatch(1000)
        .build();
    DataFrameWriter<Row> edgeDataFrameWriter = new DataFrameWriter<>(edgeDataset);
    package$.MODULE$.NebulaDataFrameWriter(edgeDataFrameWriter)
        .nebula(nebulaConnectionConfig, writeNebulaEdgeConfig).writeEdges();
    System.out.println("End to write nebula data [edge]");

  }


}
