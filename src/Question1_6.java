
import java.io.IOException;
import java.util.HashMap;

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

public class Question1_6 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private HashMap<String, Integer> map;
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String word: value.toString().split("\\s+")){
				
				// If the key 'word' already exists then increment value else create a new entry into the hashmap
				if(this.map.containsKey(word)){
					Integer oldValue = this.map.get(word);
					this.map.put(word, oldValue+1);
				}
				else{
					this.map.put(word, 1);
				}
			}
		}
		

		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			this.map = new HashMap<String, Integer>();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(String key: this.map.keySet()){
				context.write(new Text(key), new LongWritable(this.map.get(key)));
			}
			
			this.map.clear();
		}

	}
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value: values){
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		Job job = Job.getInstance(conf, "Question1_6");
		job.setJarByClass(Question1_6.class);
		
		// l'utilisation de ce combiner permet de diminuer le nombre de donnees a traiter lors du reduce
		job.setCombinerClass(MyReducer.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
