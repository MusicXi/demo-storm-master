package com.myron.storm.wordcount2.kafka.topology;

import com.myron.storm.wordcount2.kafka.bolt.PrintBolt;
import com.myron.storm.wordcount2.kafka.bolt.WordCountBolt;
import com.myron.storm.wordcount2.kafka.bolt.WordNormalizerBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class WordCountTopology {
    private static final String ZK_HOSTS = "192.168.80.128:2181";
    private static final String TOPIC = "kafka1";

    private static TopologyBuilder builder=new TopologyBuilder();

    public static void main(String[] args) throws InterruptedException {
        Config config=new Config();
        BrokerHosts zk = new ZkHosts(ZK_HOSTS);

        SpoutConfig spoutConfig = new SpoutConfig(zk,TOPIC,"", "ddd");
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("RandomSentence", kafkaSpout,2);//即对应两个executor线程
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(),2).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(),2).fieldsGrouping("WordNormalizer", new Fields("wordd"));
        builder.setBolt("Print", new PrintBolt(),1).shuffleGrouping("WordCount");
        
        config.setDebug(false);//调试模式,会把所有的log都打印出来
        
        //通过是否有参数来判断是否启动集群，或者本地模式执行
        if(args!=null&&args.length>0){
            System.out.print("集群模式------------------------------------------------");
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                // TODO: handle exception
            }
        }else{
            System.out.print("本地模式------------------------------------------------");
            //本地模式
            config.setMaxTaskParallelism(1);
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("wordcount",config,builder.createTopology() );
            
            Thread.sleep(50000L);
            //关闭本地集群
            cluster.shutdown();
        }
    }
}