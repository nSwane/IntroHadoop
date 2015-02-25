

import java.io.IOException;

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

/**
 * Determiner le nombre de pays dans un intervalle.
 * 
 * @author nawaouis
 *
 */
public class Histogram {
	
	// ---------- Job1 : Calcul du nombre de tag par pays ---------- //
	
	public static class MyMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int TAG_POSITION = 8;
			int LON_POSITION = 10;
			int LAT_POSITION = 11;
			
			long nTags;
			
			boolean formatError = false;
			String [] fields = value.toString().split("\\t");
			
			formatError |= fields[TAG_POSITION].isEmpty();
			formatError |= fields[LON_POSITION].isEmpty();
			formatError |= fields[LAT_POSITION].isEmpty();
			
			if(!formatError){
				Country country = Country.getCountryAt(Double.parseDouble(fields[LAT_POSITION]), Double.parseDouble(fields[LON_POSITION]));
				if(country != null){
					nTags = fields[TAG_POSITION].split(",").length;
					context.write(new Text(country.toString()), new LongWritable(nTags));
				}
			}
		}
		
	}
	
	public static class MyReducer1 extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value: values){
				sum += value.get();
			}
			
			// Retourne une cle {Pays, Nombre tags}
			context.write(key, new LongWritable(sum));
		}
	}
		
		// ---------- Job2 ---------- //
		
	public static class MyMapper2 extends Mapper<Text, LongWritable, Interval, Text> {

		private Interval longToInterval(long value){
			return null;
		}
		
		@Override
		protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
			context.write(longToInterval(value.get()), key);
		}
		
	}
	
	public static class MyReducer2 extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
				
		Job job = Job.getInstance(conf, "Histogram");
		job.setJarByClass(Histogram.class);
		
		job.setMapperClass(MyMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
