package TestJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by gman on 5/23/16.
 */
public class JobMofo {
    public static void main (String[] args) throws  Exception {
        Job job = Job.getInstance(new Configuration(), "testJob");
        job.setJarByClass(JobMofo.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(GMapper.class);
        job.setCombinerClass(GReduce.class);
        job.setReducerClass(GReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
    }
}
