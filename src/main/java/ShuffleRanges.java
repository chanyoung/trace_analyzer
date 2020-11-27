import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class ShuffleRanges {

    /**
     * Map
     *   lower(accessTime) upper(accessTime)
     * to
     *   hash lower(accessTime) upper(accessTime)
     */
    public static class ShuffleMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            if (tokenizer.countTokens() != 2) {
                throw new IllegalStateException("invalid mapper input rows: " + value.toString());
            }
            String lower = tokenizer.nextToken();
            String upper = tokenizer.nextToken();
            int hash = Hashing.adler32().hashInt(Integer.parseInt(lower)).asInt();
            context.write(new IntWritable(hash), new Text(lower + " " + upper));
        }
    }

    /**
     * This is just for making a single output file.
     * Reduce
     *   hash lower(accessTime) upper(accessTime)
     * to
     *   hash lower(accessTime) upper(accessTime)
     */
    public static class ShuffleReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Shuffling");
        job.setJarByClass(ShuffleRanges.class);
        job.setMapperClass(ShuffleMapper.class);
        job.setReducerClass(ShuffleReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}