分布式计算
1.分解任务，分发给多台计算机
2.共享稀有资源
3.平衡计算负载


数据挖掘常用分析方法
1.分类 依赖于已分类的数据训练集
2.估计
3.相关性分析/关联分析  分析哪些事情可能会一起发生
4.聚类  分析哪些数据可能属于一类

关联规则是最常用的数据挖掘技术，最重要的是频繁项集挖掘，包括以下概念
项集   项的集合，包含k个项的集合成为k项集
事务  事务是项的集合
支持度  事务中同时包含某项集中的百分比，这个百分比称之为支持度
置信度  包某个项时候，包含另一个项的概率
关联规则 

频繁二项集挖掘的思路(数据挖掘中关联规则最基本的) ，从一个订单中出现频率较高的商品组合  (比如除平时组合商品之外，未发现很可能会一起购买的商品)
关联规则的挖掘算法，比如:Apriori算法，以及FP-树频集算法。



### 问题
storm和kafka集成报
>java.lang.ClassNotFoundException: kafka.api.OffsetRequest解决方法

>org.apache.kafka.common.network.NetworkSend.<init>(Ljava/lang/String;Ljava/n
- 分析:Storm-kafka客户端的Kafka依赖关系在maven中被定义为provided,这意味着它不会被拉入 作为传递依赖. 这允许您使用与您的kafka集群兼容的Kafka依赖版本
当使用storm-kafka-client构建项目时,必须显式添加Kafka clients依赖关系
- 参考： https://stackoverflow.com/questions/34627302/noclassdeffounderror-kafka-api-offsetrequest


































