import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class PopularityLeague extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PopularityLeague(),
				args);
		System.exit(res);
	}
	

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);
				
        Job jobA = Job.getInstance(conf, "Popularity League");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);
//        return jobA.waitForCompletion(true) ? 0 : 1;        

        Job jobB = Job.getInstance(conf, "Top Popular Links");
        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setMapperClass(TopPopularityLinksMap.class);
        jobB.setReducerClass(TopPopularityLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitles.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
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
    
	public static class LinkCountMap extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = (value.toString()).split(":");
			StringTokenizer tokenizer = new StringTokenizer(line[1], " ");
			while (tokenizer.hasMoreTokens()) {
				Integer referredPager = new Integer(tokenizer.nextToken()
						.trim().toLowerCase());
				context.write(new IntWritable(referredPager),
						new IntWritable(1));
			}
		}
	}

	public static class OrphanPageReduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum == 0) {
				context.write(key, new IntWritable(sum));
			}
		}
	}

    public static class TopPopularityLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<PairTopLeague<Integer, String>> countToWordMap = new TreeSet<PairTopLeague<Integer, String>>();
        List<String> leagues = new ArrayList<String>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leagueFilename = "/mp2/misc/league.txt";//conf.g getString("league", "/mp2/F-output");
            //Read the file contents
        	FileReader fileReader;
    		try {
    			fileReader = new FileReader(leagueFilename);
    			BufferedReader br = new BufferedReader(fileReader);
    			String leagueList = "";
    			while ((leagueList = br.readLine()) != null) {
    				leagues.add(leagueList);
    				if(leagues.size() > 16) {
    					break;
    				}
    			}
    		} catch (Exception e) {
    			
    		}
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Integer pageCount = Integer.parseInt(value.toString());
	 		String pageId = key.toString();
	 		if(leagues.contains(pageId)) {
		 		countToWordMap.add(new PairTopLeague<Integer, String>(pageCount, pageId));
	 		}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			for (PairTopLeague<Integer, String> item : countToWordMap) {
				Integer[] pageCounts = {new Integer(item.second), item.first};
				IntArrayWritable val = new IntArrayWritable(pageCounts);
				context.write(NullWritable.get(), val);
		 	}
        }
    }

    public static class TopPopularityLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    	private TreeSet<PairTopLeague<Integer, String>> countToWordMap = new TreeSet<PairTopLeague<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
			for (IntArrayWritable val: values) {
				IntWritable[] PairTopLeague= (IntWritable[]) val.toArray();
	 			String word =  Integer.toString(PairTopLeague[0].get());
	 			Integer count = new Integer(PairTopLeague[1].get());
	 			countToWordMap.add(new PairTopLeague<Integer,String>(count, word));
	 		}
			Integer count = 0;
	 		for (PairTopLeague<Integer, String> item: countToWordMap) {
	 			IntWritable word = new IntWritable(new Integer(item.second));
	 			//IntWritable value = new IntWritable(item.first);
	 			IntWritable value = new IntWritable(count++);
	 			context.write(word, value);
	 		}
        }
    }
}

//>>> Don't Change
class PairTopLeague<A extends Comparable<? super A>,
     B extends Comparable<? super B>>
     implements Comparable<PairTopLeague<A, B>> {

 public final A first;
 public final B second;

 public PairTopLeague(A first, B second) {
     this.first = first;
     this.second = second;
 }

 public static <A extends Comparable<? super A>,
         B extends Comparable<? super B>>
 PairTopLeague<A, B> of(A first, B second) {
     return new PairTopLeague<A, B>(first, second);
 }

 @Override
 public int compareTo(PairTopLeague<A, B> o) {
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
     if (!(obj instanceof PairTopLeague))
         return false;
     if (this == obj)
         return true;
     return equal(first, ((PairTopLeague<?, ?>) obj).first)
             && equal(second, ((PairTopLeague<?, ?>) obj).second);
 }

 private boolean equal(Object o1, Object o2) {
     return o1 == o2 || (o1 != null && o1.equals(o2));
 }

 @Override
 public String toString() {
     return "(" + first + ", " + second + ')';
 }
}
//<<< Don't Change