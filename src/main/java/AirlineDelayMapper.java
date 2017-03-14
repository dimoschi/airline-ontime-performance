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
        // Read input, parse it and create a record
        CSVRecord record = CSVParser.parse(
            value.toString(), CSVFormat.EXCEL
        ).getRecords().get(0);
        /*
        Set delayTime = 0.00 and check for non-empty records to change that

        Here we make the assumption that an empty record is equal to no delay,
        but this can also be invalid. It would be better to discard those
        records.
        */
        Double delayTime = 0.00;
        String DelayTimeString = record.get(7);
        // Simple implementation to parse header line
        if (DelayTimeString.equals("DEP_DELAY")) {
            delayTime = 0.00;
        } else if (!DelayTimeString.isEmpty()) {
            delayTime = Double.parseDouble(record.get(7));
        }
        Text carrier = new Text(record.get(1));
        context.write(carrier, new DoubleWritable(delayTime));
    }
}
