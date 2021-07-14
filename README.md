# nebula-spark-java-demo

该工程为***java调用nebula-spark的相关示例,并且做了一些简单封装***，示例包含nebula-spark-connector和一些算法

nebula-spark详见：https://github.com/vesoft-inc/nebula-spark-utils/tree/v2.0.0

**本工程基于官方提供的2.0.0版本编写**

### nebula-spark-connector

spark读取nebula数据：
`com.loading.nebula.connetor.NebulaSparkReaderExample`

spark数据写入nebula：
`com.loading.nebula.connetor.NebulaSparkWriterExample`

*注意：代码中需要替换你自己相关的环境信息，例如nebula地址*

### nebula-algorithm

详见：https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-algorithm

使用方式可具体查看单元测试com.loading.nebula.lib.AlgoHandlerTest

|           算法名          |中文说明|应用场景|
 |:------------------------:|:-----------:|:----:|
 |         PageRank         |  页面排序  | 网页排序、重点节点挖掘|
 |         Louvain          |  社区发现  | 社团挖掘、层次化聚类|
 |          KCore           |    K核    |社区发现、金融风控|
 |     LabelPropagation     |  标签传播  |资讯传播、广告推荐、社区发现|
 |    ConnectedComponent    |  联通分量  |社区发现、孤岛发现|
 |StronglyConnectedComponent| 强联通分量  |社区发现|
 |       TriangleCount      | 三角形计数  |网络结构分析|
 |   BetweennessCentrality  | 介数中心性  |关键节点挖掘，节点影响力计算|
 |        DegreeStatic      |   度统计   |图结构分析|
 

