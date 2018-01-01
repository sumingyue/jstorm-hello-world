package com.itclj.topology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.utils.JStormUtils;
import com.itclj.bolt.MergeRecordBolt;
import com.itclj.bolt.PairCountBolt;
import com.itclj.bolt.SplitRecordBolt;
import com.itclj.bolt.TotalCountBolt;
import com.itclj.spout.ItcljSpout;
import com.itclj.utils.JStormHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * JStorm topology 构造拓扑
 */
public class ItcljTopology {

    private static Logger logger = LoggerFactory.getLogger(ItcljTopology.class);

    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";

    private static Map conf = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please input configuration file");
            System.exit(-1);
        }
        conf = JStormHelper.LoadConf(args[0]);
        if (JStormHelper.localMode(conf)) {
            SetLocalTopology();
        } else {
            SetRemoteTopology();
        }
    }

    public static void SetLocalTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, 1);
        SetBuilder(builder, conf);

        logger.debug("test");
        logger.info("Submit log");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SplitMerge", conf, builder.createTopology());

        Thread.sleep(60000);
        cluster.killTopology("SplitMerge");
        cluster.shutdown();
    }

    public static void SetBuilder(TopologyBuilder builder, Map conf) {

        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int bolt_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);

        builder.setSpout(ItcljTopologyDef.SEQUENCE_SPOUT_NAME, new ItcljSpout(), spout_Parallelism_hint);

        boolean isEnableSplit = JStormUtils.parseBoolean(conf.get("enable.split"), false);

        if (!isEnableSplit) {
            BoltDeclarer boltDeclarer = builder.setBolt(ItcljTopologyDef.TOTAL_BOLT_NAME, new TotalCountBolt(),
                    bolt_Parallelism_hint);

            // localFirstGrouping is only for jstorm
            // boltDeclarer.localFirstGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
            boltDeclarer.shuffleGrouping(ItcljTopologyDef.SEQUENCE_SPOUT_NAME)
                    .allGrouping(ItcljTopologyDef.SEQUENCE_SPOUT_NAME, ItcljTopologyDef.CONTROL_STREAM_ID)
                    .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        } else {

            builder.setBolt(ItcljTopologyDef.SPLIT_BOLT_NAME, new SplitRecordBolt(), bolt_Parallelism_hint)
                    .localOrShuffleGrouping(ItcljTopologyDef.SEQUENCE_SPOUT_NAME);

            builder.setBolt(ItcljTopologyDef.TRADE_BOLT_NAME, new PairCountBolt(), bolt_Parallelism_hint)
                    .shuffleGrouping(ItcljTopologyDef.SPLIT_BOLT_NAME, ItcljTopologyDef.TRADE_STREAM_ID);
            builder.setBolt(ItcljTopologyDef.CUSTOMER_BOLT_NAME, new PairCountBolt(), bolt_Parallelism_hint)
                    .shuffleGrouping(ItcljTopologyDef.SPLIT_BOLT_NAME, ItcljTopologyDef.CUSTOMER_STREAM_ID);

            builder.setBolt(ItcljTopologyDef.MERGE_BOLT_NAME, new MergeRecordBolt(), bolt_Parallelism_hint)
                    .fieldsGrouping(ItcljTopologyDef.TRADE_BOLT_NAME, new Fields("ID"))
                    .fieldsGrouping(ItcljTopologyDef.CUSTOMER_BOLT_NAME, new Fields("ID"));

            builder.setBolt(ItcljTopologyDef.TOTAL_BOLT_NAME, new TotalCountBolt(), bolt_Parallelism_hint)
                    .noneGrouping(ItcljTopologyDef.MERGE_BOLT_NAME);
        }

        /*
        boolean kryoEnable = JStormUtils.parseBoolean(conf.get("kryo.enable"), false);
        if (kryoEnable) {
            System.out.println("Use Kryo ");
            boolean useJavaSer = JStormUtils.parseBoolean(conf.get("fall.back.on.java.serialization"), true);

            Config.setFallBackOnJavaSerialization(conf, useJavaSer);

            Config.registerSerialization(conf, TradeCustomer.class, TradeCustomerSerializer.class);
            Config.registerSerialization(conf, Pair.class, PairSerializer.class);
        }
        */

        // conf.put(Config.TOPOLOGY_DEBUG, false);
        // conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
        // conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        int ackerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
        Config.setNumAckers(conf, ackerNum);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        // conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        // conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 20);
        conf.put(Config.TOPOLOGY_WORKERS, workerNum);

    }


    public static void SetRemoteTopology() throws AlreadyAliveException, InvalidTopologyException {
        String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (streamName == null) {
            streamName = "SequenceTest";
        }

        TopologyBuilder builder = new TopologyBuilder();
        SetBuilder(builder, conf);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        StormSubmitter.submitTopology(streamName, conf, builder.createTopology());
    }

}
