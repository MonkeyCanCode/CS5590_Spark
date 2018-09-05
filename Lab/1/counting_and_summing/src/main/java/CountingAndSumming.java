import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @since 2018-09-04
 */

public class CountingAndSumming {
    /**
     * Mapper class
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            String token = null;
            String[] lines = null;
            // Split line with with string ":': left part is the name (key) and right part is the time
            while (st.hasMoreTokens()) {
                token = st.nextToken();
                lines = token.split(":");
                // Emit
                context.write(new Text(lines[0]), new Text(lines[1]));
            }
        }
    }

    /**
     * Combiner class
     */
    public static class Combine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer count = 0;
            Double sum = 0.0;
            // Sum up all values for the same key
            for (Text val : values) {
                count++;
                sum += Double.parseDouble(val.toString());
            }
            // Calculate average
            Double average = sum / count;
            // Emit with (key, [average, count])
            context.write(new Text(key), new Text(average + "," + count));
        }
    }

    /**
     * Reducer class
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer totalCount = 0;
            Double sum = 0.0;
            String[] lines = null;
            // Sum up all value inside values
            for (Text val : values) {
                // Split line with with string ",': left part is the average and right part is the count
                lines = val.toString().split(",");
                Double average = Double.parseDouble(lines[0]);
                Integer count = Integer.parseInt(lines[1]);
                sum += average * count;
                totalCount += count;
            }
            // Calculate average
            Double finalAverage = sum / totalCount;
            // Emit the average time
            context.write(key, new Text(finalAverage.toString()));
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
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
