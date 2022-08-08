import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class AvgComputationCombiner extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Pair>
	{
		public static boolean isNumber(String s){
			for(int i=0;i<s.length();i++){
				if(s.charAt(i)<'0' || s.charAt(i)>'9')
					return false;
			}
			return true;
		}
		public static boolean isAdressIP(String s)
	    {
			if(s==null || s.length()==0)
				return false;
			
			String[] data  = s.split("\\.");
			if(data.length!=4)
				return false;
			
			for(int i=0;i<data.length;i++){
				if(!isNumber(data[i])|| (Integer.parseInt(data[i])<0 || Integer.parseInt(data[i])>255  ) )
					return false;
			}
			
			return true;
	    }

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Map<String,Integer> hm = new HashMap<String,Integer>();	// sum
			Map<String,Integer> cmpt = new HashMap<String,Integer>(); // number
			String line = value.toString();
			String[] data  = line.split(" ");
			String ip = data[0];
			String v = data[data.length-1];
			
			if(isAdressIP(ip) && isNumber(v)){
				//context.write(new Text(ip), new IntWritable(Integer.parseInt(v)));
				if(hm.containsKey(ip)){
					hm.put(ip, hm.get(key)+Integer.parseInt(v));
					cmpt.put(ip, cmpt.get(key)+1);
				}else{
					hm.put(ip, Integer.parseInt(v));
					cmpt.put(ip,1);
				}
			}
			
			for (@SuppressWarnings("rawtypes") Map.Entry mapElement : hm.entrySet()) 
			{
				String k = (String)mapElement.getKey();
				int val = ((int)mapElement.getValue());
				int nbr = ((int)cmpt.get(k));
				context.write(new Text(k), new Pair(String.valueOf(val),String.valueOf(nbr)));
			}

			
			
		}
	}

	public static class MyReducer extends Reducer<Text, Pair, Text, DoubleWritable>
	{
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			
			int sum = 0;
			int count = 0;
			for (Pair val : values)
			{
				count += Integer.valueOf(val.second.toString());
				sum += Integer.valueOf(val.first.toString());
			}
			
			result.set(sum);
			double avg = (double)sum/count;
			context.write(key, new DoubleWritable(avg));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AvgComputationCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AverageComputationCombiner");
		job.setJarByClass(AvgComputationCombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(Text.class);
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
