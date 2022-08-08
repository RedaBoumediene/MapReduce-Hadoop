import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StripApproach extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, MapWritable>
	{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String line = value.toString().trim();
			String[] data  = line.split(" ");
			
			for(int i=0;i<data.length;i++){
				MapWritable mp = new MapWritable();
				for(int j=i+1;j<data.length;j++){
					if(data[i].equals(data[j]))
						break;
					Text e = new Text(data[j]);
					if(mp.containsKey(e)){
						mp.put(new Text(data[j]), new DoubleWritable(((DoubleWritable) mp.get(e)).get()+1));
					}else{
						mp.put(new Text(data[j]), new DoubleWritable(1));
					}
				}
				if(mp.size()!=0)
					context.write(new Text(data[i]), mp);
			}
			
		}
	}

	public static class MyReducer extends Reducer<Text, MapWritable, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
		{
			MapWritable mp = new MapWritable();
			double cmpt = 0;
			for (MapWritable val : values)
			{
				for (Entry<Writable, Writable> entry : val.entrySet()) {
					
					if(mp.containsKey(entry.getKey())){
						double v1 =  ((DoubleWritable) mp.get(entry.getKey())).get()  ;
						double v2 = ((DoubleWritable) entry.getValue()).get();
						mp.put(entry.getKey(), new DoubleWritable(v1+v2));
					}else{
						double v2 = ((DoubleWritable) entry.getValue()).get();
						mp.put(entry.getKey(), new DoubleWritable(v2));
					}
					
					cmpt = cmpt + ((DoubleWritable) entry.getValue()).get();
				}
			}
			String ans = "";
			for (Entry<Writable, Writable> entry : mp.entrySet()) {
				double c = ((DoubleWritable)entry.getValue()).get() / cmpt;
				DoubleWritable avg = new DoubleWritable(c);
				ans = ans + "\n ( "+entry.getKey()+" , "+avg+" ) ";
			}
			context.write(key, new Text(ans));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new StripApproach(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "StripApproach");
		job.setJarByClass(StripApproach.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
