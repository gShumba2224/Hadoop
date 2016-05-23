package JsonParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import  org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class JsonMapper extends Mapper <Integer,String,IntWritable,MapWritable> {

    @Override
    protected void map(Integer key,String value, Context context) throws IOException, InterruptedException{

        MapWritable outputMap = new MapWritable();
        String[] values = (value.substring(1,value.length()-1)).split(",");
        for (String val:values){
            String[] mapping = val.replaceAll("\"","").split(":");
            outputMap.put(new Text (mapping[0]),new Text (mapping[1]));
        }
        context.write( new IntWritable(key),outputMap);
    }
}

public class JsonParser{
    public void parseFile (String inputFile,String outputFile) throws Exception{

        Job job = Job.getInstance(new Configuration(), "testJob");
        job.setJarByClass(JsonParser.class);
        FileInputFormat.addInputPath(job,new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.setMapperClass(JsonMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MapWritable.class);
    }
}

