package wc;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Aggregate {

 //  public static class AggregateMapper extends Mapper<Object, Text, NullWritable, Text>{
// //http://www.grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-mapreduce-client-core/2.4.1/org/apache/hadoop/mapreduce/Mapper.java?av=h
//     public void map(Object key, Text value, Context context
//                     ) throws IOException, InterruptedException {
//       
// //        String[] words = value.toString().split("$");
// // 
// //         for( String word: words){
// // 
// //             
// //             context.write(new Text(word), NullWritable.get());    
// //         }
// 
//             context.write(NullWritable.get(), value); 
// 
//      }
//   }

//   public static class AggregateReducer
//        extends Reducer<Text,IntWritable,Text,IntWritable> {
//     private IntWritable result = new IntWritable();
// 
//     public void reduce(Text key, Iterable<IntWritable> values,
//                        Context context
//                        ) throws IOException, InterruptedException {
//       int sum = 0;
//       for (IntWritable val : values) {
//         sum += val.get();
//       }
//       result.set(sum);
//       context.write(key, result);
//     }
//   }

	public static class ReverseTextComparator extends WritableComparator {
	 
		public ReverseTextComparator() {
			super(Text.class, true);
		}
	
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
		
// 			Text k1 = (Text)w1;
// 			Text k2 = (Text)w2;

            Integer k1 = Integer.parseInt( w1.toString());
            Integer k2 = Integer.parseInt( w2.toString());
			
			return -1 * Integer.compare(k1, k2); //k1.compareTo(k2);
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
   
    Job job = Job.getInstance(conf, "Aggregate");
    
    job.setJarByClass(Aggregate.class);
       
//  job.setMapperClass(AggregateMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setSortComparatorClass(ReverseTextComparator.class);

//    job.setCombinerClass(Reducer.class);
//    job.setReducerClass(Reducer.class);
    
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(Text.class);

	conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
	job.setInputFormatClass(KeyValueTextInputFormat.class);
    KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
