package com.alibaba.otter.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
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
    private static DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter());
    private static boolean appenderInited = false;
    private static boolean deleterInited = false;

    private static File file;
    private static HdfsAvroAppender hdfsAvroAppender;
    private static HdfsAvroAppender hdfsAvroDeleter;

    public static void setHdfsAvroDeleter(HdfsAvroAppender hdfsAvroDeleter) {
        if (!deleterInited) {
            PartionedCanalClient.hdfsAvroDeleter = hdfsAvroAppender;
            deleterInited = true;
        } else {
            return;
        }
    }

    public synchronized static void setHdfsAvroAppender(HdfsAvroAppender hdfsAvroAppender) {
        if (!appenderInited) {
            PartionedCanalClient.hdfsAvroAppender = hdfsAvroAppender;
            appenderInited = true;
        } else {
            return;
        }
    }

    public PartionedCanalClient(String destination, String schemaPath) throws IOException {
        super(destination);
        this.schema = getSchemaFromFile(schemaPath);
    }

    public synchronized static void append(GenericRecord record) throws IOException {
        logger.info("append invoked\n");
        writer.append(record);
    }

    public static Schema getSchemaFromFile(String path) throws IOException {
        File file = new File(path);
        Schema schema = Schema.parse(file);
        return schema;
    }

    @Override
    protected void processEntrys(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                    logger.info(destination + " BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});

                if (!entry.getHeader().getTableName().equals(schema.getName())) {
                    continue;
                }

                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        processColumn(rowData.getBeforeColumnsList(), true);
                    } else if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                        processColumn(rowData.getAfterColumnsList(), false);
                    } else {
                        processColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected void processColumn(List<CanalEntry.Column> columns, boolean delete) {
        GenericData.Record record = new GenericData.Record(schema);
        logger.info("create a new recode\n");
        for (CanalEntry.Column column : columns) {
            record.put(column.getName(), toAvro(SqlUtils.stringToSqlValue(column.getValue(), column.getSqlType(), column.hasIsNull(), false)));
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
        if (delete) {
            PartionedCanalClient.hdfsAvroDeleter.append(record);
            logger.info("delete a record");
        } else if (!delete) {
            PartionedCanalClient.hdfsAvroAppender.append(record);
            logger.info("update a record");
        }
    }

    public static void main(String args[]) throws IOException {
        // 根据ip，直接创建链接，无HA的功能
        String[] destinations = {"hotelmaster01", "hotelmaster02", "hotelmaster03", "hotelmaster04"};
        String confDir = System.getProperty("canal.conf.dir");
        String schemaPath = confDir + "/schema/com.elong.corp.hotel_property_master.1.avsc";
        String appendPath = "/data/hive/warehouse/ods.db/wangjiantest/slice=upsert/test.append.data";
        String deletePath = "/data/hive/warehouse/ods.db/wangjiantest/slice=delete/test.delete.data";
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://namenode001.hadoop.bjy.elong.com:9000");
        HdfsAvroAppender hdfsAvroAppender = new HdfsAvroAppender(appendPath, conf, getSchemaFromFile(schemaPath));
        HdfsAvroAppender hdfsAvroDeleter = new HdfsAvroAppender(deletePath, conf, getSchemaFromFile(schemaPath));
        hdfsAvroAppender.init();
        hdfsAvroDeleter.init();
        setHdfsAvroAppender(hdfsAvroAppender);
        setHdfsAvroDeleter(hdfsAvroDeleter);


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
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        });
    }

    private Object toAvro(Object o) {
        boolean bigDecimalFormatString = false;
        if (o instanceof BigDecimal) {
            if (bigDecimalFormatString) {
                return ((BigDecimal) o).toPlainString();
            } else {
                return o.toString();
            }
        } else if (o instanceof java.sql.Date) {
            return ((java.sql.Date) o).getTime();
        } else if (o instanceof Time) {
            return ((Time) o).getTime();
        } else if (o instanceof Timestamp) {
            return ((Timestamp) o).getTime();
        }
        // primitive types (Integer, etc) are left unchanged
        return o;
    }

}