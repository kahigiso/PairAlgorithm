package main.pack;

import helper.pack.WordPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapPairApproach extends Mapper<LongWritable, Text, WordPair, IntWritable> {

	private  static IntWritable occur = new IntWritable(1);
	private Map<String, Integer> mp = new HashMap<String, Integer>();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> itemIds = Arrays.asList(value.toString().trim().split(" "));			
		for(int k=0; k<itemIds.size();k++){
			String item = itemIds.get(k);
			List<String> neighbors = getNeighbors(k, itemIds) ;
			for(String neighbor : neighbors){
				if(mp.containsKey(item)) mp.put(item,mp.get(item)+1);
				else mp.put(item,1);
				System.out.println("Mapper - ("+item+", "+neighbor+") occur: "+occur);
				context.write(new WordPair(item,neighbor), occur);
			}
		}
		
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	   WordPair pair = new WordPair();
	   for (String pid : mp.keySet()) {
		   pair.setKey(new Text(pid));
		   pair.setValue(new Text("*"));
		   System.out.println(" (Mapper- ("+pair.getKey()+", "+pair.getValue()+") occur: "+occur);
		   context.write(pair, new IntWritable(mp.get(pid)));
	   }
	}	
	private List<String> getNeighbors(Integer pos, List<String> items){
		List<String>  neighbors = new ArrayList<String>();
		if(pos == items.size()-1) return neighbors;
		for(int i =pos+1; i< items.size(); i++){
			if(items.get(i).equals(items.get(pos))) break;
			else neighbors.add(items.get(i));
		}
		return neighbors;
	}

}
