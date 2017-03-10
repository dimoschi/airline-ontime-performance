import java.io.*;
import java.util.*;
import org.apache.commons.csv.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class AirlineDelayMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        // Read input
        Reader in = new StringReader(value.toString());
        CSVParser parser = new CSVParser(in, CSVFormat.EXCEL);
        List<CSVRecord> record = parser.getRecords();
        // Check for empty records
        String isDelayedString = record.get(0).get(8).toString();
        Double delayTime = 0.00;
        if (!isDelayedString.isEmpty()) {
            delayTime = Double.parseDouble(record.get(0).get(7).toString());
        }
        String airline = record.get(0).get(1).toString();
        context.write(new Text(airline), new DoubleWritable(delayTime));
    }
}
