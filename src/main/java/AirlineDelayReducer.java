import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Iterables;

public class AirlineDelayReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            Integer sizeOfValues = Iterables.size(values);
            Double sumOfValues = 0.00;
            for (DoubleWritable value : values) {
                Double delay = value.get();
                sumOfValues = sumOfValues + delay;
            }
            Double avgOfValues = sumOfValues/sizeOfValues;
            context.write(key, new DoubleWritable(avgOfValues));
        }
    }
