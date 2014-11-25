package com.alibaba.otter.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import org.apache.avro.Schema;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * 单机模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class PartionedCanalClient extends AbstractCanalClient {
    private Schema schema;

    public PartionedCanalClient(String destination, String schemaPath) throws IOException {
        super(destination);
        this.schema = getSchemaFromFile(schemaPath);
    }

    public static Schema getSchemaFromFile(String path) throws IOException {
        File file = new File(path);
        Schema schema = Schema.parse(file);
        logger.info(schema.getFullName());
        return schema;
    }

    public static void main(String args[]) throws IOException {
        // 根据ip，直接创建链接，无HA的功能
        String[] destinations = {"hotelmaster01", "hotelmaster02", "hotelmaster03", "hotelmaster04"};
        String schemaPath = "conf/schema/com.elong.corp.hotel_base_master.1.avsc";

        final List<PartionedCanalClient> clients = new LinkedList<PartionedCanalClient>();

        for (String destination : destinations) {
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                    11111), destination, "", "");
            final PartionedCanalClient clientTest = new PartionedCanalClient(destination, schemaPath);
            clientTest.setConnector(connector);
            clientTest.start();
            clients.add(clientTest);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                for (PartionedCanalClient clientTest : clients) {
                    try {
                        logger.info("## stop the canal client");
                        clientTest.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        logger.info("## canal client is down.");
                    }
                }
            }

        });
    }

}