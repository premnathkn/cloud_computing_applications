import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Pre Top Popular Links");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Popular Links");
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopPopularLinksMap.class);
        jobB.setReducerClass(TopPopularLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitles.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] line = (value.toString()).split(":");
        	StringTokenizer tokenizer = new StringTokenizer(line[1], " ");
        	while (tokenizer.hasMoreTokens()) {
			 	Integer referredPager = new Integer(tokenizer.nextToken().trim().toLowerCase());
			 	context.write(new IntWritable(referredPager), new IntWritable(1));
        	}
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	int sum = 0;
	 		for (IntWritable val : values) {
	 			sum += val.get();
	 		}
	        context.write(key, new IntWritable(sum));
        }
    }

    public static class TopPopularLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    	Integer N = 0;
        private TreeSet<PairTop<Integer, String>> countToWordMap = new TreeSet<PairTop<Integer, String>>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Integer pageCount = Integer.parseInt(value.toString());
	 		String pageId = key.toString();
	 		countToWordMap.add(new PairTop<Integer, String>(pageCount, pageId));
	 		if (countToWordMap.size() > this.N) {
	 			countToWordMap.remove(countToWordMap.first());
	 		}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			for (PairTop<Integer, String> item : countToWordMap) {
				Integer[] pageCounts = {new Integer(item.second), item.first};
				IntArrayWritable val = new IntArrayWritable(pageCounts);
				context.write(NullWritable.get(), val);
		 	}
        }
    }

    public static class TopPopularLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
    	private TreeSet<PairTop<Integer, String>> countToWordMap = new TreeSet<PairTop<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
			for (IntArrayWritable val: values) {
				IntWritable[] PairTop= (IntWritable[]) val.toArray();
	 			String word =  Integer.toString(PairTop[0].get());
	 			Integer count = new Integer(PairTop[1].get());
	 			countToWordMap.add(new PairTop<Integer,String>(count, word));
	 			if (countToWordMap.size() > this.N) {
					countToWordMap.remove(countToWordMap.first());
	 			}
	 		}
	 		for (PairTop<Integer, String> item: countToWordMap) {
	 			IntWritable word = new IntWritable(new Integer(item.second));
	 			IntWritable value = new IntWritable(item.first);
	 			context.write(word, value);
	 		}
        }
    }
}

// >>> Don't Change
class PairTop<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<PairTop<A, B>> {

    public final A first;
    public final B second;

    public PairTop(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    PairTop<A, B> of(A first, B second) {
        return new PairTop<A, B>(first, second);
    }

    @Override
    public int compareTo(PairTop<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PairTop))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((PairTop<?, ?>) obj).first)
                && equal(second, ((PairTop<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change