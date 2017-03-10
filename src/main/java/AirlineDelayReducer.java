import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Iterables;
import org.apache.log4j.Logger;

public class AirlineDelayReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {

            // Initialize values
            int countOfFlights = 0;
            int countOfDelayedFlights = 0;
            double sumOfDelays = 0.00;
            double avgOfDelays = 0.00;
            double percOfDelay = 0.00;

            for (DoubleWritable value : values) {
                countOfFlights += 1;
                double delay = value.get();
                if (delay >= 15.00) {
                    countOfDelayedFlights += 1;
                    sumOfDelays += delay;
                }
                avgOfDelays = sumOfDelays/countOfDelayedFlights;
                percOfDelay = ((double) countOfDelayedFlights)/countOfFlights;
            }
            context.write(key, new DoubleWritable(avgOfDelays));
            context.write(key, new DoubleWritable(percOfDelay));
        }
    }
