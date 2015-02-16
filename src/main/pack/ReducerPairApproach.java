package main.pack;

import helper.pack.WordPair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPairApproach extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {

	private Integer devider = 1;
	private final Text flag = new Text("*");

	public void reduce(WordPair pair, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		Integer occur = 0;
		for (IntWritable value : values)
			if(pair.getValue().equals(flag))
				devider = value.get();
			else{ 
				occur += value.get();
			}
		if(!pair.getValue().equals(flag)){
			System.out.println("Reducer-  ("+pair.getKey()+", "+pair.getValue()+") occur: "+(new DoubleWritable((double)occur/devider)).get());
			context.write(pair,new DoubleWritable((double)occur/devider));
		}
			
	}
	
}
