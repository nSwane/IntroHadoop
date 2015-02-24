
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

public class Question2_1 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

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
						context.write(new Text(country.toString()), new Text(tag));
					}
				}
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// Map a tag to its number of occurence
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			// Initialize map
			for(Text tag: values){
				if(map.containsKey(tag.toString())){
					int oldValue = map.get(tag.toString());
					map.put(tag.toString(), oldValue+1);
				}
				else{
					map.put(tag.toString(), 1);
				}
			}
			
			// Number of tags per country
			int k = Integer.parseInt(context.getConfiguration().get("K"));
			
			// Retreive only the k-th first max
			MinMaxPriorityQueue<StringAndInt> queue = MinMaxPriorityQueue.maximumSize(k).create();
			
			// Methode 1
//			for(String tag: map.keySet()){
//				queue.add(new StringAndInt(tag, map.get(tag)));
//			}
			
			// Methode 2
			for(Entry<String, Integer> entry: map.entrySet()){
				queue.add(new StringAndInt(entry.getKey(), entry.getValue()));
			}
			
			// Return results
			while(!queue.isEmpty()){
				StringAndInt o = queue.removeFirst();
				context.write(new Text(key), new Text(java.net.URLDecoder.decode(o.getTag(), "UTF-8")+" "+map.get(o.getTag())));
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
		
		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

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
