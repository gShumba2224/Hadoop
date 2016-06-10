package TestJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import JsonParser.JsonMapper;
/**
 * Created by gman on 5/23/16.
 */
public class JobMofo {
    public static void main (String[] args) throws  Exception {
        Job job = Job.getInstance(new Configuration(), "testJob");
        job.setJarByClass(JobMofo.class);

        Configuration mapConfig = new Configuration(false);
        ChainMapper.addMapper(job,JsonMapper.class,LongWritable.class,Text.class,LongWritable.class,MapWritable.class,mapConfig);

        mapConfig = new Configuration(false);
        ChainMapper.addMapper(job,GMapper.class,LongWritable.class,MapWritable.class,IntWritable.class,IntWritable.class,mapConfig);

        String in = "/home/gman/Documents/IntelijWorkspace/SparkStreaming/src/main/resources/Datasets/SourceData/CleanTrainingSet/";
        String out = "/home/gman/Documents/IntelijWorkspace/SparkStreaming/src/main/resources/Datasets/SourceData/output";
        FileInputFormat.addInputPath(job,new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setCombinerClass(GReduce.class);
        job.setReducerClass(GReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
