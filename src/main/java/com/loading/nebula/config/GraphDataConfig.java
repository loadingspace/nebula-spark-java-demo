package com.loading.nebula.config;

import com.typesafe.config.Config;
import java.util.List;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class GraphDataConfig {

  private String space;

  private String edgeName;

  private String vertexName;

  private boolean vertexNoCol = true;

  private List<String> vertexCols;

  private int partitionNum = 10;

  public GraphDataConfig(){

  }

  public GraphDataConfig(Config config){
    this.space = config.getString("space");
    this.edgeName = config.getString("edgeName");
    this.vertexName = config.getString("vertexName");
    this.vertexNoCol = config.getBoolean("vertexNoCol");
    this.vertexCols = config.getStringList("vertexCols");
    this.partitionNum = config.getInt("partitionNum");
  }

  public String getSpace() {
    return space;
  }

  public void setSpace(String space) {
    this.space = space;
  }

  public String getEdgeName() {
    return edgeName;
  }

  public void setEdgeName(String edgeName) {
    this.edgeName = edgeName;
  }

  public String getVertexName() {
    return vertexName;
  }

  public void setVertexName(String vertexName) {
    this.vertexName = vertexName;
  }

  public boolean isVertexNoCol() {
    return vertexNoCol;
  }

  public void setVertexNoCol(boolean vertexNoCol) {
    this.vertexNoCol = vertexNoCol;
  }

  public List<String> getVertexCols() {
    return vertexCols;
  }

  public void setVertexCols(List<String> vertexCols) {
    this.vertexCols = vertexCols;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }
}
