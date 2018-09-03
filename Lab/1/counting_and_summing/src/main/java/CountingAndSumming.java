import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Lab 1 part 2: compute average time
 *
 * @author Yong Zheng
 * @since 2018-09-02
 */

public class CountingAndSumming {
    /**
     * Mapper class
     */
    public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            String token = null;
            String[] lines = null;
            // Split line with ':': left part is the name (key) and right part is the time
            while (st.hasMoreTokens()) {
                token = st.nextToken();
                lines = token.split(":");
                // Emit
                context.write(new Text(lines[0]), new FloatWritable(Float.parseFloat(lines[1])));
            }
        }
    }

    /**
     * Reducer class
     */
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            // Sum up all value inside values
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            // Emit the average time
            context.write(key, new FloatWritable(sum / count));
        }
    }

    /**
     * Main class
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Counting and Summing");

        job.setJarByClass(CountingAndSumming.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
