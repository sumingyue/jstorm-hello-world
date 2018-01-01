package com.itclj.utils;

import backtype.storm.Config;
import com.alibaba.jstorm.utils.LoadConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class JStormHelper {
    private final static Logger logger = LoggerFactory.getLogger(JStormHelper.class);

    public static Map LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            return LoadConf.LoadYaml(arg);
        } else {
            return LoadConf.LoadProperty(arg);
        }
    }

    public static boolean localMode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }

        return false;
    }
}
