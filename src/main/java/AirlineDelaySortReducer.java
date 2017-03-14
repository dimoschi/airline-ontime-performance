import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AirlineDelaySortReducer
extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    public void reduce(Text key, Text value, Context context)
    throws IOException, InterruptedException {
        context.write(key, value);
    }
}
