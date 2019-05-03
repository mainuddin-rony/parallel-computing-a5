package wc;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;
import org.apache.hadoop.mapreduce.lib.map.*;

import java.util.regex.Matcher;

public class SortByCount {

  public static class MyMapper
       extends Mapper<Object, Text, IntWritable, Text>{

		Pattern pattern = Pattern.compile("^(\\p{L}+)\\h*(\\d*)$");
		IntWritable intw =new IntWritable();

		@Override
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

			String text = value.toString();
			Matcher matcher = pattern.matcher(text);
			
			while(matcher.find()) {
				intw.set(Integer.parseInt(matcher.group(2)));
				context.write(intw, new Text(matcher.group(1)));	
			}
		}
  }
  
  

    public static class CountReducer
       extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable total = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
    
          int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
  
          total.set(sum);
          context.write(total, key);
        }
    }

	public static class ReverseTextComparator extends WritableComparator {
	 
		public ReverseTextComparator() {
			super(Text.class, true);
		}
	
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text)w1;
			Text k2 = (Text)w2;
			
			return -1 * super.compare(k1, k2); //k1.compareTo(k2);
		}
	}

	public static class ReverseIntWritableComparator extends WritableComparator {
	 
		public ReverseIntWritableComparator() {
			super(IntWritable.class, true);
		}
	
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable k1 = (IntWritable)w1;
			IntWritable k2 = (IntWritable)w2;
			
			return -1 * super.compare(k1, k2);
		}
	}


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Sort By Count");
    job.setJarByClass(SortByCount.class);

//    job.setMapperClass(MyMapper.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(MyMapper.class);

    //conf.setMapOutputKeyClass(Text.class); 
  	//conf.setMapOutputValueClass(IntWritable.class); 

    job.setCombinerClass(CountReducer.class);
    job.setReducerClass(CountReducer.class);
    job.setNumReduceTasks(16);
    
    job.setSortComparatorClass(ReverseIntWritableComparator.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
// 	conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
// 	job.setInputFormatClass(KeyValueTextInputFormat.class);
	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
