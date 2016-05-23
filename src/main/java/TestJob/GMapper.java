package TestJob; /**
 * Created by gman on 5/23/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import  org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class GMapper extends Mapper <Integer,Integer,IntWritable,IntWritable> {

    @Override
    protected void map(Integer key,Integer value, Context context) throws IOException, InterruptedException{
       context.write(new IntWritable(key),new IntWritable(value));
    }
}
