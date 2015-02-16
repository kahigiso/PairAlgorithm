package main.pack;

import helper.pack.WordPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairApproachMain {

	public static void main(String[] args) throws Exception {

//		if(args.length!=2){
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//		}
		System.out.println("========Pair Algorithm Approach=========");
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(MapPairApproach.class);
		job.setReducerClass(ReducerPairApproach.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(DoubleWritable.class);
	
		Path outDir = new Path("files/output");
		FileSystem.get(job.getConfiguration()).delete(outDir,true);
        FileInputFormat.setInputPaths(job, new Path("files/input"));
        FileOutputFormat.setOutputPath(job, outDir);
        
//    	FileInputFormat.setInputPaths(job, new Path(args[0]));
//      FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setJarByClass(PairApproachMain.class);
        job.setJobName("Pair Algorithm Approach");
        
        System.exit(job.waitForCompletion(true)?0:1);

	}


}
