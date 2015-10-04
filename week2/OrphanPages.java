import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change
//# hadoop jar OrphanPages.jar OrphanPages /mp2/links /mp2/D-output
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    		List<Integer> allPager = new ArrayList<Integer>();
	        @Override
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        	String[] line = (value.toString()).split(":");
	        	allPager.add(new Integer(line[0]));
	        	StringTokenizer tokenizer = new StringTokenizer(line[1], " ");
	        	while (tokenizer.hasMoreTokens()) {
				 	Integer referredPager = new Integer(tokenizer.nextToken().trim().toLowerCase());
				 	context.write(new IntWritable(referredPager), new IntWritable(1));
				 	if(allPager.contains(referredPager)) {
				 		allPager.remove(referredPager);
				 	}
	        	}
	        }

	        @Override
	        protected void cleanup(Context context) throws IOException, InterruptedException {
				for (Integer pageNo : allPager) {
					context.write(new IntWritable(pageNo), new IntWritable(0));
			 	}
	        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int sum = 0;
     		for (IntWritable val : values) {
     			sum += val.get();
     		}
     		if(sum == 0) {
                context.write(key, NullWritable.get());
     		}
        }
    }
}