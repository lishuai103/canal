package com.alibaba.otter.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.net.InetSocketAddress;

/**
 * 单机模式的测试例子
 * 
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class SimpleCanalClient extends AbstractCanalClient {

    public SimpleCanalClient(String destination){
        super(destination);
    }

    public static void main(String args[]) {
        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                                                                                            11111), destination, "", "");

        final SimpleCanalClient clientTest = new SimpleCanalClient(destination);
        clientTest.setConnector(connector);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }

}
