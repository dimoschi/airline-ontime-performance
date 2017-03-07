import java.io.*;
import java.util.*;
import org.apache.commons.csv.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AirlineDelayMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        Reader in = new StringReader(value.toString());
        // To-add Headers
        CSVParser parser = new CSVParser(in, CSVFormat.EXCEL);
        List<CSVRecord> record = parser.getRecords();
        String isDelayedString = record.get(0).get(8).toString();
        if (!isDelayedString.isEmpty()) {
            Double isDelayed = Double.parseDouble(isDelayedString);
            if (isDelayed == 1.00) {
                String airline = record.get(0).get(1).toString();
                Double delayTime = Double.parseDouble(record.get(0).get(7).toString());
                context.write(
                    new Text(airline), new DoubleWritable(delayTime)
                );
            }
        }
    }
}
