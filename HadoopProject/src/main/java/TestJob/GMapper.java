package TestJob; /**
 * Created by gman on 5/23/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GMapper extends Mapper <LongWritable,MapWritable,IntWritable,IntWritable> {

    @Override
    protected void map(LongWritable key,MapWritable value, Context context) throws IOException, InterruptedException{
        Text propType = (Text)value.get(new Text("PropertyType"));
        if (propType != null) {
            int newKey = Integer.parseInt((propType).toString());
            context.write(new IntWritable(newKey), new IntWritable(1));
        }
    }
}
