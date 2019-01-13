package joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class tupleKey implements WritableComparable<tupleKey>{
	
	final public static int R = 0; 
	final public static int L = 1; 
	
	IntWritable tag = new IntWritable();
	Text key = new Text();
	
	public tupleKey() {
		// TODO Auto-generated constructor stub
	}
	
	public tupleKey(int _tag, String _key) {
		// TODO Auto-generated constructor stub
		tag.set(_tag);
		key.set(_key);
	}
	
	public int noTagCompareTo(tupleKey o){
		return key.compareTo(o.key);
	}
	
	public IntWritable getTag(){
		return tag;
	}
	
	public Text getKey(){
		return key;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		tag.readFields(arg0);
		key.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		tag.write(arg0);
		key.write(arg0);
	}

	@Override
	public int compareTo(tupleKey o) {
		// TODO Auto-generated method stub
		int cmp = key.compareTo(o.key);
		return (cmp != 0) ? cmp : tag.compareTo(o.getTag());
	}

}
