package JsonParser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class JsonMapper extends Mapper <LongWritable,Text,LongWritable,MapWritable> {

    @Override
    protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{

        MapWritable outputMap = new MapWritable();
        String strValue = value.toString();
        String[] values = (strValue.substring(1,strValue.length()-1)).split(",");
        for (String val:values){
            String[] mapping = val.replaceAll("\"","").split(":");
            outputMap.put(new Text (mapping[0]),new Text (mapping[1]));
           // System.out.println ("jason "+mapping[0]+" ::: "+mapping[1]);
        }
        context.write(key,outputMap);
    }
}

