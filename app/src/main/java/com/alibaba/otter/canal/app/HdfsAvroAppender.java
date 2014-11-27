package com.alibaba.otter.canal.app;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wangjian
 * Date: 2014-11-26
 * Time: 15:57:00
 */

public class HdfsAvroAppender {
    Log LOG = LogFactory.getLog(HdfsAvroAppender.class);
    private String hdfsFilePath;
    private FileSystem fs;
    private Configuration conf;
    private Schema schema;
    private FSDataOutputStream out;
    private DataFileWriter<GenericRecord> writer =
            new DataFileWriter<GenericRecord>(new GenericDatumWriter()).setSyncInterval(100);

    public HdfsAvroAppender(String hdfsFilePath, Configuration conf, Schema schema) throws IOException {
        this.hdfsFilePath = hdfsFilePath;
        this.conf = conf;
        this.fs = FileSystem.newInstance(conf);
        this.schema = schema;
    }

    public boolean isADataFile(String hdfsFilePath, FileSystem fs) throws IOException {
        Path path = new Path(hdfsFilePath);
        FSDataInputStream inputStream = fs.open(path);
        AvroFSInput avroFSInput = new AvroFSInput(inputStream, fs.getFileStatus(path).getLen());
        boolean isAdataFile = false;
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            dataFileReader =
                    new DataFileReader<GenericRecord>(avroFSInput, datumReader);
        } catch (IOException e) {
            if (e.getMessage().equals("Not a data file.")) {
                dataFileReader = null;
            } else {
                throw e;
            }
        }
        if (null != dataFileReader) {
            isAdataFile = true;
        }
        return isAdataFile;
    }


    public void init() throws IOException {
        Path f = new Path(hdfsFilePath);
        if (fs.exists(f)) {
            fs.rename(f, new Path(hdfsFilePath + "." + System.currentTimeMillis()));
            f = new Path(hdfsFilePath);
        }
        out = fs.create(f);
        writer.create(this.schema, out);
    }

    public synchronized void append(GenericRecord record) {
        try {
            LOG.info("start append a record for " + record.getSchema().getName());
            writer.append(record);
            //在这里调用hflush 太重了，应该另外起一个线程，单独做；
            writer.flush();
            writer.fSync();
            out.hflush();
            out.hsync();
            LOG.info("end append a record for " + record.getSchema().getName());

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("append error");
        }
    }

}
