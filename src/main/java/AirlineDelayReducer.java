import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Iterables;
import org.apache.log4j.Logger;

public class AirlineDelayReducer
extends Reducer<Text, DoubleWritable, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
    throws IOException, InterruptedException {

        // Initialize values
        int countOfFlights = 0;
        int countOfDelayedFlights = 0;
        double sumOfDelays = 0.00;
        // Iterate through the values (delayTime) of a key (carrier)
        for (DoubleWritable value: values) {
            // Count the total values (flights) of a key (carrier)
            countOfFlights += 1;
            double delay = value.get();
            /*
            Check if the flight was delayed and if true, add one to the
            the DelayedFlights counter and to the total amount of delay
            */
            if (delay >= 15.00) {
                countOfDelayedFlights += 1;
                sumOfDelays += delay;
            }
        }
        double avgOfDelays = sumOfDelays/countOfDelayedFlights;
        double percOfDelay = ((double) countOfDelayedFlights)/countOfFlights;
        Text outputValue = new Text(avgOfDelays + ";" + percOfDelay);
        context.write(key, outputValue);
    }
}
