package SillyPackage;
import org.apache.commons.compress.utils.CharsetNames;
import org.apache.directory.api.util.ByteBuffer;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HBaseQuery {

    private static DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

    public static  Map<String,Double> getRange(String tableName,String familyName,String columnName,long minTimeStamp, long maxTimeStamp, Double min, Double max) throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setTimeRange(minTimeStamp,maxTimeStamp);
        scan.addColumn(familyName.getBytes(),columnName.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        HashMap<String,Double> outMap = new HashMap<String, Double>();
        Result result;
        while ((result = scanner.next())!= null){
            String rowKey = new  String(result.getRow(), CharsetNames.UTF_8);
            String valueStr = new String( result.getValue(familyName.getBytes(),columnName.getBytes()), CharsetNames.UTF_8);
            Double price = Double.valueOf(valueStr);
            if (price >= min && price < max) outMap.put(rowKey,price);
        }
        return outMap;
    }

    private static long parseTimeDate (String dateString) throws ParseException {
        return dateFormat.parse(dateString).getTime();
    }

    public static void main (String args[]) throws Exception{

        String[] columnPath = args[0].split(";");
        String[] timeRange = args[1].split(";");
        String[] numRange = args[2].split(";");

        Map<String,Double> queryResult = getRange(columnPath[0],columnPath[1],columnPath[2],
                parseTimeDate(timeRange[0]),parseTimeDate(timeRange[1]),
                Double.valueOf(numRange[0]),Double.valueOf(numRange[1]));

        Iterator <Map.Entry<String,Double>>it =  queryResult.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry <String,Double> entry = it.next();
            System.out.println(entry.getKey()+" "+entry.getValue());
        }

    }
}
