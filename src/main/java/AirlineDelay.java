import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirlineDelay {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                "Usage: AirlineDelay <input path> " +
                "<intermediate path> <output path>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Airline Delay");
        job1.setJarByClass(AirlineDelay.class);
        job1.setMapperClass(AirlineDelayMapper.class);
        job1.setReducerClass(AirlineDelayReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Airline Delay Sort");
        job2.setJarByClass(AirlineDelay.class);
        job2.setMapperClass(AirlineDelaySortMapper.class);
        job2.setReducerClass(AirlineDelaySortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
