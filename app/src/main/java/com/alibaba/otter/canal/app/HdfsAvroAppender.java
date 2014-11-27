package com.alibaba.otter.canal.app;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
    private String hdfsFilePath;
    private FileSystem fs;
    private Configuration conf;
    private Schema schema;
    private FSDataOutputStream out;
    private static DataFileWriter<GenericRecord> writer =
            new DataFileWriter<GenericRecord>(new GenericDatumWriter()).setSyncInterval(1000);

    public HdfsAvroAppender(String hdfsFilePath, Configuration conf, Schema schema) throws IOException {
        this.hdfsFilePath = hdfsFilePath;
        this.conf = conf;
        this.fs = FileSystem.get(conf);
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
        if (!fs.exists(f)) {
            FSDataOutputStream outstream = fs.create(f);
            outstream.close();
        }
        out = fs.append(f);

        if (!isADataFile(hdfsFilePath, fs)) {
            writer.create(this.schema, out);
        }
    }

    public synchronized static void append(GenericRecord record) {
        try {
            writer.append(record);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("append error");
        }
    }

}