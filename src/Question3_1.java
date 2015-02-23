
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question3_1 {

	// ---------- Job1 ---------- //
	
	public static class MyMapper1 extends Mapper<LongWritable, Text, CountryTag, LongWritable> {

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
						context.write(new CountryTag(country.toString(), tag), new LongWritable(1));
					}
				}
			}
		}
		
	}
	
	public static class MyReducer1 extends Reducer<CountryTag, LongWritable, CountryTag, LongWritable> {
		
		@Override
		protected void reduce(CountryTag key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value: values){
				sum += value.get();
			}
			
			context.write(new CountryTag(key.getCountry().toString(), key.getTag().toString()), new LongWritable(sum));
		}
	}
	
	// ---------- Job2 ---------- //
	
	public static class MyMapper2 extends Mapper<CountryTag, LongWritable, CountryFrequency, Text> {

		@Override
		protected void map(CountryTag key, LongWritable value, Context context) throws IOException, InterruptedException {
			context.write(new CountryFrequency(key.getCountry().toString(), value.get()), new Text(key.getTag()));
		}
		
	}
	
	public static class MyReducer2 extends Reducer<CountryFrequency, Text, CountryTag, LongWritable> {
		
		@Override
		protected void reduce(CountryFrequency key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int i, k;
			Iterator<Text> it_values;
			
			// Number of tags per country: top k
			k = Integer.parseInt(context.getConfiguration().get("K"));
			i = 0;
			it_values = values.iterator();
			while(i < k && it_values.hasNext()){
				Text tag = it_values.next();
				context.write(new CountryTag(key.getCountry().toString(), tag.toString()), new LongWritable(key.getFrequency()));
				i++;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k = otherArgs[2];
		String tmp = "Question3_1_DIR";
		boolean exit_state = true;
		
		// Set parameter for reduce call
		conf.set("K", k);
		
		// ---------- Job1 ---------- //
		
		Job job = Job.getInstance(conf, "Question3_1");
		job.setJarByClass(Question3_1.class);
		
		job.setMapperClass(MyMapper1.class);
		job.setMapOutputKeyClass(CountryTag.class);
		job.setMapOutputValueClass(LongWritable.class);

		//job.setCombinerClass(MyCombiner.class);
		
		job.setReducerClass(MyReducer1.class);
		job.setOutputKeyClass(CountryTag.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(tmp));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Wait the job to finish
		exit_state = job.waitForCompletion(true);
		
		// ---------- Job2 ---------- //
		
		Job job2 = Job.getInstance(conf, "Question3_1");
		job2.setJarByClass(Question3_1.class);
		
		// Grouper par pays
		job2.setGroupingComparatorClass(CountryComparator.class);
		
		// Trier par frequence
		job2.setSortComparatorClass(FrequencyComparator.class);
		
		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(CountryFrequency.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(CountryTag.class);
		job2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(tmp+"/part-r-00000"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		// Wait the job to finish
		exit_state &= job2.waitForCompletion(true);
		
		System.exit(exit_state ? 0 : 1);
	}
}
