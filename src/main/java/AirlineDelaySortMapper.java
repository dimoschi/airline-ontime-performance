import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class AirlineDelaySortMapper
extends Mapper<Object, Text, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
        String line = value.toString();
        /*
        Split the input with space, where data[0] is the carrier and
        data[1] is the delayTime and data[2] is the percOfDelay
        */
        String[] data = line.split("\\s+");
        /*
        The new key is the percOfDelay because we want the output to be
        sorted by that
        */
        context.write(new Text(data[2]), new Text(data[0] + " " + data[1]));
    }
}
