package helper.pack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable, WritableComparable<WordPair>{
	
	private Text key;
	private Text value;
	
	public  WordPair(Text key, Text value) {
		this.key = key;
		this.value = value;
	}
	
	public WordPair(String key, String value) {
		this(new Text(key),new Text(value));
	}
	
	public  WordPair() {
		this.key = new Text();
		this.value = new Text();
	}
	
	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	@Override
	public int compareTo(WordPair word) {
		int val = this.key.compareTo(word.getKey());
		
		if(val!=0){
			return val;
		}
		if(this.value.toString().equals("*")){
			return -1;
		}else if(word.getValue().toString().equals("*")){
			return 1;
		}
		
		return this.value.compareTo(word.getValue());
	}

	public static WordPair read(DataInput in) throws IOException{
		WordPair pair = new WordPair();
		pair.readFields(in);
		return pair;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}
	
	@Override
	public String toString(){
		return "("+key+","+value+")";
	}

	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + ((key == null) ? 0 : key.hashCode());
		result = 31 * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
		
		WordPair other = (WordPair) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	

}
