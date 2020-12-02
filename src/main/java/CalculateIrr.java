import com.google.common.collect.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.graph.Graphs;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toCollection;

public class CalculateIrr {

    /**
     * Map
     *   HashKey lower(accessTime) upper(accessTime)
     * to
     *   lower(accessTime) upper(accessTime)
     * to <- cleanup(Context)
     *   lower(accessTime) irr
     */
    public static class IrrMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        Comparator<Range<Integer>> comparator = Comparator.comparingInt((Range<Integer> range) -> {
            return range.hasLowerBound() ? range.lowerEndpoint() : range.upperEndpoint();
        });
        NavigableSet<Range<Integer>> ranges = new TreeSet<>(comparator);
        List<Integer> accessTimes = new ArrayList<>();
        List<Range<Integer>> executionOrder;

        static final Log log = LogFactory.getLog(IrrMapper.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            if (tokenizer.countTokens() != 3) {
                throw new IllegalStateException("invalid mapper input rows: " + value.toString());
            }
            tokenizer.nextToken(); // Drop hash key.
            int lower = Integer.parseInt(tokenizer.nextToken());
            int upper = Integer.parseInt(tokenizer.nextToken());
            ranges.add(Range.closed(lower, upper));
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load trace data.
            String tracePath = context.getConfiguration().get("tracepath");

            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (
                    FSDataInputStream inputStream = fs.open(new Path(tracePath));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            ) {
                String key = reader.readLine();
                while (key != null) {
                    accessTimes.add(Integer.valueOf(key));
                    key = reader.readLine();
                }
            }
        }

        private void makeExecutionOrder() {
            Network<Range<Integer>, Integer> dependencyGraph = getDependencies(ranges);
            log.info("generate graph done");
            executionOrder = topologicalSort(dependencyGraph);
            log.info("topological sort done, jobs: " + executionOrder.size());
            ranges.clear();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Integer> uniqueKeys = new HashSet<>();
            Range<Integer> prevRange = Range.closed(0, accessTimes.size());

            makeExecutionOrder();
            int executionSize = executionOrder.size();

            int executed = 0;
            int subrangeProcessed = 0;
            while (!executionOrder.isEmpty()) {
                Range<Integer> range = executionOrder.remove(0);
                checkState(accessTimes.get(range.lowerEndpoint()).equals(accessTimes.get(range.upperEndpoint())));

                if (range.encloses(prevRange)) {
                    log.info(range + " encloses " + prevRange);
                    subrangeProcessed++;
                    for (int i = range.lowerEndpoint(); i < prevRange.lowerEndpoint(); i++) {
                        uniqueKeys.add(accessTimes.get(i));
                    }
                    for (int i = prevRange.upperEndpoint(); i < range.upperEndpoint(); i++) {
                        uniqueKeys.add(accessTimes.get(i));
                    }
                } else {
                    uniqueKeys = new HashSet<>();
                    for (int i = range.lowerEndpoint(); i < range.upperEndpoint(); i++) {
                        uniqueKeys.add(accessTimes.get(i));
                    }
                }
                prevRange = range;

                int irr = uniqueKeys.size() - 1;
                context.write(new IntWritable(range.lowerEndpoint()), new IntWritable(irr));

                executed++;
                if (executed % 100 == 0) {
                    log.info("progress: " + 100 * executed / executionSize + "%"
                            + ", subrange processed: " + 100 * subrangeProcessed / executed + "%");
                }
            }
        }

        /** Returns a directed graph of the ranges so that successors can depend on prior computations. */
        private ImmutableNetwork<Range<Integer>, Integer> getDependencies(
                NavigableSet<Range<Integer>> ranges) {
            ImmutableNetwork.Builder<Range<Integer>, Integer> network =
                    NetworkBuilder.directed().immutable();
            for (Range<Integer> range : ranges) {
                network.addNode(range);
            }

            Set<Range<Integer>> pointedRanges = new HashSet<>();

            int counter = 0;
            for (Range<Integer> range : ranges) {
                Range<Integer> lowerBound = Range.atLeast(range.lowerEndpoint());
                Range<Integer> upperBound = Range.atMost(range.upperEndpoint());

                SortedSet<Range<Integer>> subSet = ranges.subSet(lowerBound, upperBound);
                int minDiff = Integer.MAX_VALUE;
                Range<Integer> largestSubset = null;
                for (Range<Integer> subRange : subSet) {
                    if (subRange.upperEndpoint() > range.upperEndpoint()) {
                        break;
                    } else if (!range.equals(subRange) && !pointedRanges.contains(subRange)) {
                        int diff = subRange.lowerEndpoint() - range.lowerEndpoint();
                        diff += range.upperEndpoint() - subRange.upperEndpoint();
                        if (diff < minDiff) {
                            minDiff = diff;
                            largestSubset = subRange;
                        }
                    }
                }
                if (largestSubset != null) {
                    network.addEdge(range, largestSubset, counter++);
                    pointedRanges.add(largestSubset);
                }
            }
            return network.build();
        }

        /**
         * Topological sort using Kahn's algorithm.
         *
         * @param graph the directed dependency graph
         * @return the list of vertices in topological order for visiting
         * @throws IllegalStateException if the network has cycles
         */
        private static <V, E> List<V> topologicalSort(Network<V, E> graph) {
            MutableNetwork<V, E> network = Graphs.copyOf(graph);
            Deque<V> sources = network.nodes().stream()
                    .filter(vertex -> network.inDegree(vertex) == 0)
                    .collect(toCollection(ArrayDeque::new));
            List<V> sorted = new ArrayList<>(network.nodes().size());

            while (!sources.isEmpty()) {
                V vertex = sources.removeLast();
                sorted.add(vertex);

                List<E> edges = ImmutableList.copyOf(network.outEdges(vertex));
                for (E edge : edges) {
                    V destination = network.incidentNodes(edge).target();
                    network.removeEdge(edge);
                    if (network.inDegree(destination) == 0) {
                        sources.addLast(destination);
                    }
                }
            }
            checkState(network.edges().isEmpty(), "Cycles detected in %s", network.edges());
            return Lists.reverse(sorted);
        }
    }

    /**
     * This is just for making a single output file.
     * Reduce
     *   lower(accessTime) irr
     * to
     *   lower(accessTime) irr
     */
    public static class IrrReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("tracepath", args[2]);
        conf.set("mapred.max.split.size", "100000");
        Job job = Job.getInstance(conf, "CalculateIrr");
        job.setJarByClass(CalculateIrr.class);
        job.setMapperClass(IrrMapper.class);
        job.setReducerClass(IrrReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Fix reducer number to 1.
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}