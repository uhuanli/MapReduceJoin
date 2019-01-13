package joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class tupleVal implements Writable{

	IntWritable tag = new IntWritable();
	Text val = new Text();
	
	public tupleVal() {
		// TODO Auto-generated constructor stub
	}
	
	public tupleVal(int _tag, String _val) {
		// TODO Auto-generated constructor stub
		tag.set(_tag);
		val.set(_val);
	}
	
	public Text getVal(){
		return val;
	}
	
	public IntWritable getTag(){
		return tag;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		tag.readFields(arg0);
		val.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		tag.write(arg0);
		val.write(arg0);
	}

}
