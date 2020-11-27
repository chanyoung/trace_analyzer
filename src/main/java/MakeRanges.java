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

public class MakeRanges {

    /**
     * Map
     *   key accessTime accessTime accessTime ...
     * to
     *   lower(accessTime) upper(accessTime)
     *   lower(accessTime) upper(accessTime)
     *   lower(accessTime) upper(accessTime)
     */
    public static class RangeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            // Drop key.
            tokenizer.nextToken();

            List<Integer> accessTimes = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                accessTimes.add(Integer.valueOf(tokenizer.nextToken()));
            }
            Collections.sort(accessTimes);

            Iterator<Integer> it = accessTimes.iterator();
            int previous = it.next();
            while (it.hasNext()) {
                int current = it.next();
                context.write(new IntWritable(previous), new IntWritable(current));
                previous = current;
            }
        }
    }

    /**
     * This is just for making a single output file.
     * Reduce
     *   lower(accessTime) upper(accessTime)
     * to
     *   lower(accessTime) upper(accessTime)
     */
    public static class RangeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MakeRanges");
        job.setJarByClass(MakeRanges.class);
        job.setMapperClass(RangeMapper.class);
        job.setReducerClass(RangeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}