package com.itclj.bean;

import shade.storm.org.apache.commons.lang.builder.ToStringBuilder;
import shade.storm.org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

public class TradeCustomer implements Serializable {
    private static final long serialVersionUID = -1139180233944190841L;

    protected final long      timestamp;
    protected Pair            trade;
    protected Pair            customer;
    protected String          buffer;

    public TradeCustomer() {
        timestamp = System.currentTimeMillis();
    }

    public TradeCustomer(long timestamp, Pair trade, Pair customer, String str) {
        this.timestamp = timestamp;
        this.trade = trade;
        this.customer = customer;
        this.buffer = str;
    }

    public Pair getTrade() {
        return trade;
    }

    public void setTrade(Pair trade) {
        this.trade = trade;
    }

    public Pair getCustomer() {
        return customer;
    }

    public void setCustomer(Pair customer) {
        this.customer = customer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getBuffer() {
        return buffer;
    }

    public void setBuffer(String buffer) {
        this.buffer = buffer;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
