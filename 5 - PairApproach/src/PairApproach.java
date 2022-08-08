import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairApproach extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Pair, IntWritable>
	{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String line = value.toString().trim();
			String[] data  = line.split(" ");
			
			for(int i=0;i<data.length;i++){
				for(int j=i+1;j<data.length;j++){
					if(data[i].equals(data[j]))
						break;
					Pair p = new Pair(data[i],data[j]);
					Pair q = new Pair(data[i],"*");
					context.write(p, new IntWritable(1));
					context.write(q, new IntWritable(1));
				}
			}
			
		}
	}

	public static class MyReducer extends Reducer<Pair, IntWritable, Text, DoubleWritable>
	{
		private static int tot = 0;
		@Override
		public void reduce(Pair pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			
			int sum = 0;
			for (IntWritable v : values)
			{
				sum += v.get();
			}
			
			if(pair.second.toString().equals("*"))
				tot  = sum ;
			else
				context.write(new Text(pair.first.toString()), new DoubleWritable((double)sum / tot));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new PairApproach(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "PairApproach");
		job.setJarByClass(PairApproach.class);
		
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(1);
		
		Configuration conf = new Configuration();
		
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		
		// delete existing directory
		if (hdfs.exists(output)) {
		  hdfs.delete(output, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
