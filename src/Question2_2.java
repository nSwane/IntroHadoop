
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int TAG_POSITION = 8;
			int LON_POSITION = 10;
			int LAT_POSITION = 11;
			
			boolean formatError = false;
			String [] fields = value.toString().split("\\t");
			
			formatError |= fields[TAG_POSITION].isEmpty();
			formatError |= fields[LON_POSITION].isEmpty();
			formatError |= fields[LAT_POSITION].isEmpty();
			
			if(!formatError){
				Country country = Country.getCountryAt(Double.parseDouble(fields[LAT_POSITION]), Double.parseDouble(fields[LON_POSITION]));
				if(country != null){
					for(String tag: fields[TAG_POSITION].split(",")){
						context.write(new Text(country.toString()), new StringAndInt(tag, 1));
					}
				}
			}
		}
		
	}
	
	public static class Util {
		
		// Map a tag to its number of occurence
		private static HashMap<String, Integer> doSum(Iterable<StringAndInt> values){
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			// Sum
			for(StringAndInt si: values){
				
				if(map.containsKey(si.getTag())){
					int oldValue = map.get(si.getTag());
					map.put(si.getTag(), oldValue+1);
				}
				else{
					map.put(si.getTag(), si.getNumberOcc());
				}
			}
			
			return map;
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> map = Util.doSum(values);
			
			// Output
			for(Entry<String, Integer> entry: map.entrySet()){
				context.write(key, new StringAndInt(entry.getKey(), entry.getValue()));
			}
			
			map.clear();
		}
	}
	
	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> map = Util.doSum(values);
			
			// Create priority queue
			
			// Number of tags per country
			int k = Integer.parseInt(context.getConfiguration().get("K"));
			
			// Retreive only the k-th first max
			MinMaxPriorityQueue<StringAndInt> queue = MinMaxPriorityQueue.maximumSize(k).create();
			
			for(Entry<String, Integer> entry: map.entrySet()){
				queue.add(new StringAndInt(entry.getKey(), entry.getValue()));
			}			
			
			// Return results
			while(!queue.isEmpty()){
				StringAndInt o = queue.removeFirst();
				context.write(new Text(key), new Text(java.net.URLDecoder.decode(o.getTag(), "UTF-8") + " "+map.get(o.getTag())));
			}
			
			map.clear();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k = otherArgs[2];
		
		// Set parameter for reduce call
		conf.set("K", k);
		
		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_2.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setCombinerClass(MyCombiner.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
