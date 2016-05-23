package TestJob; /**
 * Created by gman on 5/23/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GReduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    @Override
    protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
        int result = 0;
        for (IntWritable val : values)result = val.get()+result;
        context.write(key,new IntWritable(result));
    }
}
