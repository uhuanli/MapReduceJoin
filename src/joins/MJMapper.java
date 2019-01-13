package joins;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MJMapper extends MapReduceBase
implements Mapper<LongWritable, Text, tupleKey, tupleVal>
{
	public static boolean debug_mode = true;
	public static boolean path_mode = true;
	public static String[] Rscheme;
	public static String[] Lscheme;
	public static String rFolder;
	public static String lFolder;
	
	//Key from R is formed by R's key-column in the order as rKeyi
	//if rKeyi is {3, 2, 4} then rKey is :  rTuple[3]+"\t"+rTuple[2]+"\t"+rTuple[4]
	//for a tuple  a b c d e
	//its key is : 'c b d'
	public static int[] rKeyi;
	public static int[] lKeyi;
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<tupleKey, tupleVal> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		join(key, value, output, reporter);
	}
	
	public void join(LongWritable key, Text value,
			OutputCollector<tupleKey, tupleVal> output, Reporter reporter) throws IOException{
		String _line = value.toString();
		String[] _sp = null;
		String _key = null;
		int _tag = 0;
		{
			if(MJMapper.isRtuples(reporter)){
				_sp = splitLine(_line, Rscheme.length);
				_key = MJMapper.getKey(rKeyi, _sp);
				_tag = multiJoin.R;
			}else{
				_sp = splitLine(_line, Lscheme.length);
				_key = MJMapper.getKey(lKeyi, _sp);
				_tag = multiJoin.L;
			}
		}
		{
			StringBuilder _sb = new StringBuilder();
			_sb.append(_sp[0]);
			for(int i = 1; i < _sp.length; i ++){
				_sb.append("\t").append(_sp[i]);
			}
			tuVal = new tupleVal(_tag, _sb.toString());
		}
		output.collect(new tupleKey(_tag, _key), tuVal);
	}
	
	tupleKey tuKey = null;
	tupleVal tuVal = null;
	
	public static String[] splitLine(String _line, int _length){
		{
			if(_line.indexOf("\t") != -1){//if inputFile is not the origin one
				return _line.split("\t");
			}
		}
		String[] _ret = new String[_length];
		int _begin = 0;
		int _end = -1;
		int _count = 0;
		while( (_end = _line.indexOf(" ", _end+1)) != -1){
			{
				char _c1 = _line.charAt(_end-1);
				char _c2 = _line.charAt(_end+1);
				if(_c1 != '>' && _c1 != '"' ) continue;
				if(_c2 != '<' && _c2 != '"') continue;
			}
			_ret[_count] = _line.substring(_begin, _end);
			_count ++;
			_begin = _end + 1;
		}
		if(_count >= _length){
			System.out.print("bug line: " + _line + " len:"+_length + "\n");
		}
		else
		{
			_ret[_count] = _line.substring(_begin);
		}
		return _ret;
	}
	
	public static boolean isRtuples(Reporter reporter){
		String filePath=((FileSplit)reporter.getInputSplit()).getPath().toString();
		if(path_mode){
			if(_ii < 5 && debug_mode){
				System.out.print("rFolder : "+ rFolder + "\n");
				System.out.print("lFolder : "+ lFolder + "\n");
				System.out.print("path : "+ filePath + "\n");
				_ii ++;
			}
			if(filePath.indexOf(rFolder) != -1){
				return true;
			}
			return false;
		}
		/*	*/
		int lat1 = filePath.lastIndexOf("/");
		int lat2 = filePath.lastIndexOf(filePath, lat1-1);
		{
			if(debug_mode){
				System.out.println("iFolder:" + filePath.substring(lat2+1, lat1));
			}
		}
		if(filePath.substring(lat2+1, lat1).equals(rFolder))
			return true;
		else {
			return false;
		}
	}
	
	public static String getKey(int[] keyi, String[] tuple){
		StringBuilder _sb = new StringBuilder();
		_sb.append(tuple[ keyi[0] ]);
		for(int i = 1; i < keyi.length; i ++){
			_sb.append("\t").append(tuple[ keyi[i] ]);
		}
		return _sb.toString();
	}
	
	@Override
	public void configure(JobConf _job){
		int keyiNum = 0;
		{
			Rscheme = _job.getStrings(multiJoin.rScheTag);
			Lscheme = _job.getStrings(multiJoin.lScheTag);
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
						System.out.print("Rscheme"+i + " : " + Rscheme[i] + "\n");
					}
					for(int i = 0; i < Lscheme.length; i ++){
						System.out.print("Lscheme"+i + " : " + Lscheme[i] + "\n");
					}
				}
			}
		}
		{
			rFolder = _job.get(multiJoin.rFolderTag);
			lFolder = _job.get(multiJoin.lFolderTag);
			if(debug_mode)
			{
				
			}
		}
		{
			//Key from R is formed by R's some key-column values in the order as rKeyi indicates
			//if rKeyi is {3, 2, 4} then rKey is made up of :  rTuple[3]+"\t"+rTuple[2]+"\t"+rTuple[4]
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
	
	public static int _ii = 0;

}
