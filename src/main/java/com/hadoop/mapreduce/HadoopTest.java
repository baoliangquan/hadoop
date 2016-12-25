package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


/**
 * Created by bao on 2016/12/14.
 */
public class HadoopTest {
    private FileSystem fs;

    @Before
    public void init() throws Exception {
        Configuration con = new Configuration();
       // con.set("fs.defualtFS","hdfs://ns1/");
        fs = FileSystem.get(new URI("hdfs://ns1/"), con, "hadoop");
       // fs = FileSystem.get(con);
    }

    @Test
    public void uploadFile() throws IOException {
        fs.copyFromLocalFile(new Path("E:\\sqoop.tar.gz"), new Path("/mydata/sqoop.tar.gz"));
    }

    @Test
    public void deleteFile() throws IOException {
        boolean res = fs.delete(new Path("/mydata/sqoop.tar.gz"), true);
        System.out.println(res);
    }

    @Test
    public void mkdirs() throws IOException {
        fs.mkdirs(new Path("/mydata/aa/bb"));
    }

    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/jdk.tgz"), new Path("/jdk.tgz.rename"));
    }
    @Test
    public void List() throws IOException {
        RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path("/mydata"), true);
        while (listFile.hasNext()){
            LocatedFileStatus fileStatus = listFile.next();
            System.out.println(fileStatus.getPath().getName());;
        }
        FileStatus[] status = fs.listStatus(new Path("/mydata"));
        for(FileStatus file: status){
            System.out.println(file.getPath().getName() + "   " + (file.isDirectory()?"d":"f"));
        }
    }
}