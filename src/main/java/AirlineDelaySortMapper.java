import java.io.*;
import java.util.*;
import org.apache.commons.csv.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class AirlineDelaySortMapper
    extends Mapper<LongWritable, Text, Text, Text> {

        private Logger logger = Logger.getLogger(this.getClass());

        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            /*
            Split the input with space, where data[0] is the carrier and
            data[1] is the delayTime;percOfDelay
            */
            String[] data = line.split("\\s+");
            // Split data[1]
            String[] values = data[1].split(";");
            /*
            The new key is the percOfDelay because we want the output to be
            sorted by that
            */
            context.write(new Text(values[1]), new Text(data[0] + " " + values[0]));
        }
    }
