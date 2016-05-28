package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;


public class ReadData {
    public static void main (String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf.customCluster/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf.customCluster/hdfs-site.xml"));

        String uri =  ("hdfs://quickstart.cloudera/Datasets/datapackage.json");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(uri));
        ElasticByteBufferPool buffer = new ElasticByteBufferPool();
        byte[] bytes = new byte[4096];
        in.readFully(bytes);
        in.read();
        String str = new String (bytes,"UTF-8");
        System.out.println(str);
    }
}
