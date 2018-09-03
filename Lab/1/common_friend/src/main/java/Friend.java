import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Lab 1 part 1: compute command friends with MR
 *
 * @author Yong Zheng
 * @since 2018-09-02
 */

public class Friend {
    /**
     * Mapper class
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Generate tokenizer that use new line character
            StringTokenizer st = new StringTokenizer(value.toString(), "\n");
            String token = null;
            String[] lines = null;
            String[] friends = null;
            String[] inputKeyArray = null;
            while (st.hasMoreTokens()) {
                token = st.nextToken();
                // Split token into lines
                lines = token.split(" -> ");
                // Split right line into friend list
                friends = lines[1].split(" ");
                inputKeyArray = new String[2];
                // Emit key-value pair for each friend in friend list
                for (int i = 0; i < friends.length; i++) {
                    // Construct input key
                    inputKeyArray[0] = friends[i];
                    inputKeyArray[1] = lines[0];
                    // Sort the key before emit
                    Arrays.sort(inputKeyArray);
                    // Emit
                    context.write(new Text(inputKeyArray[0] + " " + inputKeyArray[1]), new Text(lines[1]));
                }
            }
        }
    }

    /**
     * Reducer class
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // There will be two items in the value for each key, store them into separate array
            Iterator itr = values.iterator();
            String[] lFriends = itr.next().toString().split(" ");
            String[] rFriends = itr.next().toString().split(" ");
            // Create an array to hold the final result
            ArrayList<String> commandFriends = new ArrayList<String>();
            // Check for command friend between two arrays
            for (String lFriend : lFriends) {
                for (String rFriend : rFriends) {
                    if (lFriend.equals(rFriend)) {
                        commandFriends.add(lFriend);
                    }
                }
            }
            // Build the final output for command friend
            StringBuilder sb = new StringBuilder();
            sb.append(" -> ");
            for (String friend : commandFriends) {
                sb.append(friend).append(" ");
            }
            // Emit
            context.write(key, new Text(sb.toString().trim()));
        }
    }

    /**
     * Main class
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Command Friend");

        job.setJarByClass(Friend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

