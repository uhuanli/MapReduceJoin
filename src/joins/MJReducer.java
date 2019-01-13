package joins;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MJReducer extends MapReduceBase
implements Reducer<tupleKey, tupleVal, Text, Text>
{
	public static boolean debug_mode = false;
	public static String[] Rscheme;
	public static String[] Lscheme;
	public static String rFolder;
	public static String lFolder;
	
	public static Text tKey = new Text();
	public static Text tVal = new Text();
	
	//Key from R is formed by R's key-column in the order as rKeyi
	//if rKeyi is {3, 2, 4} then rKey is :  rTuple[3]+"\t"+rTuple[2]+"\t"+rTuple[4]
	public static int[] rKeyi;
	public static int[] lKeyi;
	
	public static HashSet<String> hsStr = null;
	/*
	 * once values.next() is called, value of _key(where _key points to) is reRead
	 * if function setOutputValueGroupingComparator() is used, 
//	 * _key's beging reRead may make much sense
	 */
	@Override
	public void reduce(tupleKey _key, Iterator<tupleVal> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		join(_key, values, output, reporter);
	}
	
	HashSet<String> delDup = new HashSet<String>();
	
	public void join(tupleKey _key, Iterator<tupleVal> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		hsStr = new HashSet<String>();
		tupleVal _tv = values.next();
		int firstTag = _tv.getTag().get();
		hsStr.add(_tv.getVal().toString());
		delDup.clear();
		while(values.hasNext()){
			_tv = values.next();
			if(_tv.getTag().get() == firstTag)
			{
				hsStr.add(_tv.getVal().toString());
			}
			else
			{
				if(delDup.contains(_tv.getVal().toString())) continue;
				delDup.add(_tv.getVal().toString());
				for(String iStr : hsStr)
				{
					tKey.set( joinValues(iStr, _tv.getVal()) );
					output.collect(tKey, null);
				}
			}
		}
	}
	
	/* <a b c d e>  join <w b v d x> = <a b c d e w v x> */
	public String joinValues(String _l_val, Text _r_val){
		StringBuilder _sb = new StringBuilder();
		_sb.append(_l_val);
		String[] _sp = _r_val.toString().split(multiJoin.TabDelimiter);
		if(_sp.length == rKeyi.length) return _sb.toString();
		{
			boolean isKeypart = false;
			for(int i = 0; i < _sp.length; i ++){
				for(int j = 0; j < rKeyi.length; j ++){
					if(i == rKeyi[j]){
						isKeypart = true;
					}
				}
				if(isKeypart){
					isKeypart = false;
					continue;
				}
				_sb.append("\t").append(_sp[i]);
			}
		}
		return _sb.toString();
	}
	
	
	@Override
	public void configure(JobConf _job){
		int keyiNum = 0;
		{
			Rscheme = _job.getStrings("rScheme");
			Lscheme = _job.getStrings("lScheme");
			for(int i = 0; i < Rscheme.length; i ++){
				for(int j = 0; j < Lscheme.length; j ++){
					if(Rscheme[i].equals(Lscheme[j])){
						keyiNum ++;
						break;
					}
				}
			}
			
			{
				if(debug_mode){
					for(int i = 0; i < Rscheme.length; i ++){
						System.out.print("Rscheme"+i + " : [" + Rscheme[i] + "]\n");
					}
					for(int i = 0; i < Lscheme.length; i ++){
						System.out.print("Lscheme"+i + " : [" + Lscheme[i] + "]\n");
					}
				}
			}
		}
		{
			rFolder = _job.get("rFolder");
			lFolder = _job.get("lFolder");
		}
		{
			//Key from R is formed by R's key-column in the order as rKeyi
			//if rKeyi is {3, 2, 4} then rKey is :  rTuple[3]+"\t"+rTuple[2]+"\t"+rTuple[4]
			//for a tuple  a b c d e
			//its key is : 'c b d'
			rKeyi = new int[keyiNum];
			lKeyi = new int[keyiNum];
			int _count_keyi = 0;
			for(int i = 0; i < Rscheme.length; i ++){
				String _ri = Rscheme[i];
				for(int j = 0; j < Lscheme.length; j ++){
					String _li = Lscheme[j];
					{
						if(_ri.equals(_li)){
							rKeyi[_count_keyi] = i;
							lKeyi[_count_keyi] = j;
							_count_keyi ++;
						}
					}
				}
			}
		}
	}

}
