import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class AccessTimeCount {

    /**
     * Map
     *   key
     * to
     *   key accessTime
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length != 2) {
                throw new IllegalStateException("invalid mapper input rows: " + value.toString());
            }
            String lineNum = tokens[0];
            String block = tokens[1];
            context.write(new Text(block), new IntWritable(Integer.parseInt(lineNum)));
        }
    }

    /**
     * Reduce
     *   key accessTime
     * to
     *   Key accessTime accessTime accessTime ...
     */
    public static class AccessTimeReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (IntWritable val : values) {
                sb.append(val.get()).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * Make line number embedded trace file.
     */
    private static void embeddingLineNum(String inputPath, String outputPath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(outputPath))) {
            return;
        }
        try (
                FSDataInputStream inputStream = fs.open(new Path(inputPath));
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                FSDataOutputStream outputStream = fs.create(new Path(outputPath));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        ) {
            String key = reader.readLine();
            int lineNum = 0;
            while (key != null) {
                writer.write(lineNum + "," + key + "\n");
                key = reader.readLine();
                lineNum++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Pre-processing for making trace file with the line(operation) numbers are embedded.
        String inputDir = FilenameUtils.getPath(args[0]);
        String lineNumFileName = FilenameUtils.getBaseName(args[0]) + ".lineNum.txt";
        final String embedLineNumInputPath = FilenameUtils.concat(inputDir, lineNumFileName);
        embeddingLineNum(args[0], embedLineNumInputPath);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountAccessTimes");
        job.setJarByClass(AccessTimeCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AccessTimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(embedLineNumInputPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Reducer number is fixed to 1 to make a single output.
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}