package com.itclj.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.utils.JStormUtils;
import com.itclj.bean.Pair;
import com.itclj.bean.PairMaker;
import com.itclj.bean.TradeCustomer;
import com.itclj.topology.ItcljTopologyDef;
import com.itclj.utils.TpsCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class ItcljSpout implements IRichSpout {
    public static final Logger logger = LoggerFactory.getLogger(ItcljSpout.class);

    SpoutOutputCollector collector;
    private TpsCounter tpsCounter;
    private AsmCounter tpCounter;

    private boolean isLimited = false;
    private boolean isSendCtrlMsg = false;
    private boolean isFinished;
    private int bufferLen = 0;
    private long tupleId;
    private long succeedCount;
    private long failedCount;
    private Random random;
    private Random idGenerate;

    private long SPOUT_MAX_SEND_NUM;
    private Long MAX_PENDING_COUNTER;

    private AtomicLong tradeSum = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(0);
    private AtomicLong handleCounter = new AtomicLong(0);


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;

        if (conf.get("spout.max.sending.num") == null) {
            isLimited = false;
        } else {
            isLimited = true;
            SPOUT_MAX_SEND_NUM = JStormUtils.parseLong(conf.get("spout.max.sending.num"));
        }

        Boolean btrue = JStormUtils.parseBoolean(conf.get("spout.send.contrl.message"));
        if (btrue != null && btrue) {
            isSendCtrlMsg = true;
        } else {
            isSendCtrlMsg = false;
        }

        isFinished = false;

        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());

        MAX_PENDING_COUNTER = getMaxPending(conf);

        bufferLen = JStormUtils.parseInt(conf.get("byte.buffer.len"), 0);

        random = new Random();
        random.setSeed(System.currentTimeMillis());

        idGenerate = new Random(Utils.secureRandomLong());

        JStormUtils.sleepMs(20 * 1000);

        logger.info("Finish open, buffer Len:" + bufferLen);
    }

    @Override
    public void close() {
        tpsCounter.cleanup();
        logger.info("Sending :" + tupleId + ", success:" + succeedCount + ", failed:" + failedCount);
        logger.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
    }

    @Override
    public void activate() {
        logger.info("Start active");
    }

    @Override
    public void deactivate() {
        logger.info("Start deactive");
    }

    @Override
    public void nextTuple() {
        if (!isLimited) {
            emit();
            return;
        }

        if (isFinished) {
            if (isSendCtrlMsg) {
                // LOG.info("spout will send control message due to finish
                // sending ");
                long now = System.currentTimeMillis();
                String ctrlMsg = "spout don't send message due to pending num at " + now;
                collector.emit(ItcljTopologyDef.CONTROL_STREAM_ID, new Values(String.valueOf(now)), ctrlMsg);
            }
            logger.info("Finish sending ");
            JStormUtils.sleepMs(500);
            return;
        }

        if (tupleId > SPOUT_MAX_SEND_NUM) {
            isFinished = true;
            return;
        }

        emit();
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public long getMaxPending(Map conf) {
        // if single spout thread, MAX_PENDING should be Long.MAX_VALUE
        if (ConfigExtension.isSpoutSingleThread(conf)) {
            return Long.MAX_VALUE;
        }

        Object pending = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if (pending == null) {
            return Long.MAX_VALUE;
        }

        int pendingNum = JStormUtils.parseInt(pending);
        if (pendingNum == 1) {
            return Long.MAX_VALUE;
        }

        return pendingNum;
    }

    public void emit() {
        String buffer = null;
        if (bufferLen > 0) {
            byte[] byteBuffer = new byte[bufferLen];

            for (int i = 0; i < bufferLen; i++) {
                byteBuffer[i] = (byte) random.nextInt(200);
            }
            buffer = new String(byteBuffer);
        }

        Pair trade = PairMaker.makeTradeInstance();
        Pair customer = PairMaker.makeCustomerInstance();

        TradeCustomer tradeCustomer = new TradeCustomer();
        tradeCustomer.setTrade(trade);
        tradeCustomer.setCustomer(customer);
        tradeCustomer.setBuffer(buffer);

        tradeSum.addAndGet(trade.getValue());
        customerSum.addAndGet(customer.getValue());

        collector.emit(new Values(idGenerate.nextLong(), tradeCustomer), tupleId);
        tupleId++;
        handleCounter.incrementAndGet();
        tpCounter.inc();
        while (handleCounter.get() >= MAX_PENDING_COUNTER - 1) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }

        tpsCounter.count();

    }
}
