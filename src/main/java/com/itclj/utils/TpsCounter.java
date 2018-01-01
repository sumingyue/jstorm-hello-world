package com.itclj.utils;

import com.alibaba.jstorm.utils.IntervalCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TpsCounter implements Serializable {
    private static final long serialVersionUID = 2177944366059817622L;
    private AtomicLong total = new AtomicLong(0);
    private AtomicLong times = new AtomicLong(0);
    private AtomicLong values = new AtomicLong(0);

    private IntervalCheck intervalCheck;

    private final String id;
    private final Logger LOG;

    public TpsCounter() {
        this("", TpsCounter.class);
    }

    public TpsCounter(String id) {
        this(id, TpsCounter.class);
    }

    public TpsCounter(Class tclass) {
        this("", tclass);
    }

    public TpsCounter(String id, Class tclass) {
        this.id = id;
        this.LOG = LoggerFactory.getLogger(tclass);

        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60);
    }

    public Double count(long value) {
        long totalValue = total.incrementAndGet();
        long timesValue = times.incrementAndGet();
        long v = values.addAndGet(value);

        Double pass = intervalCheck.checkAndGet();
        if (pass != null) {
            times.set(0);
            values.set(0);

            Double tps = timesValue / pass;

            StringBuilder sb = new StringBuilder();
            sb.append(id);
            sb.append(", tps:" + tps);
            sb.append(", avg:" + ((double) v) / timesValue);
            sb.append(", total:" + totalValue);
            LOG.info(sb.toString());

            return tps;
        }

        return null;
    }

    public Double count() {
        return count(1L);
    }

    public void cleanup() {
        LOG.info(id + ", total:" + total);
    }

    public IntervalCheck getIntervalCheck() {
        return intervalCheck;
    }
}
