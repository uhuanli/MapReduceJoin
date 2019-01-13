package joins;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.sun.xml.internal.bind.v2.runtime.RuntimeUtil.ToStringAdapter;

public class multiJoin {
	final public static int R = 1;
	final public static int L = 0;
	final public static String rFolderTag = "rFolder";
	final public static String lFolderTag = "lFolder";
	final public static String rScheTag = "rScheme";
	final public static String lScheTag = "lScheme";
	public static int tempTableCount = 0;
	public static String spaceORtab = " |\t";
	public static String TabDelimiter = "\t";
	
	public static void main(String[] args) throws IOException{
		multiJoin(args);
	}
	
	/*
	 * args : multiJoin tableNum tablePath1,tableScheme1 tablePath2,tableScheme2.... outputPath
	 */
	
	public static int tableNumIndex = 1;
	public static void multiJoin(String[] args) throws IOException{
		int tableNum = 0;
		Table[] allTables;
		String outputPath;
		{/* parse input to form allTables */
			
			tableNum = Integer.parseInt(args[tableNumIndex]);
			outputPath = args[args.length-1];
			{
				if(args.length != (tableNum*2+3)){
					System.out.println("bugs in input format");
					System.exit(0);
				}
			}
			allTables = new Table[tableNum];
			for(int i = 0; i < allTables.length; i ++){
				String _path = args[tableNumIndex + (2*i+1)];
				String[] _scheme = args[tableNumIndex + (2*i+2)].split(",");
				allTables[i] = new Table(_scheme, _path);
			}
		}
		{/* do the scheduling */
			
		}
		{/* the join */
			Table _t = allTables[0];
			for(int i = 1; i < allTables.length; i ++){
				if(i == allTables.length-1)
				{
					_t = _t.join(allTables[i], outputPath);
				}
				else
				{
					_t = _t.join(allTables[i], multiJoin.getNewTablePath());
				}
			}
		}
	}
	
	public static void joinMR(Table _tL, Table _tR, String newTablePath) throws IOException{


		String _input1, _input2, _output;
		{
			_input1 = _tL.path;
			_input2 = _tR.path;
			_output = newTablePath;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(multiJoin.class);
		conf.setJarByClass(multiJoin.class);
		{
		}
		// TODO: specify output types
		conf.setOutputKeyClass(tupleKey.class);
		conf.setOutputValueClass(tupleVal.class);
		// TODO: specify input and output DIRECTORIES (not files)
		{
			multiJoin.deleteOutputPath(conf, _output);
			
			conf.set(multiJoin.lFolderTag, _input1);
			conf.set(multiJoin.rFolderTag, _input2);
			conf.setStrings(multiJoin.rScheTag, _tR.getScheme());
			conf.setStrings(multiJoin.lScheTag, _tL.getScheme());
		}
		FileInputFormat.setInputPaths(conf, new Path(_input1), new Path(_input2)); 
		FileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(MJMapper.class);
		conf.setReducerClass(MJReducer.class);
		{
			conf.setPartitionerClass(mjPartitioner.class);
			conf.setOutputValueGroupingComparator(mjGroupComparator.class);
		}
		
		conf.setNumMapTasks(9);
		conf.setNumReduceTasks(9);
		long splitSize = 1 * 1024 * 1024;
		conf.set("mapred.min.split.size", "" + splitSize);
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	public static String getNewTablePath(){
		tempTableCount ++;
		return "tempTable"+tempTableCount;
	}
	
	
	private static void deleteOutputPath(JobConf conf, String _output) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(_output)))
		{
			fs.delete(new Path(_output));
		}
	}
}

class Table{
	String[] scheme;
	String path;
	
	public Table(String[] _scheme, String _path) {
		// TODO Auto-generated constructor stub
		scheme = _scheme.clone();
		path = _path;
	}
	
	public String scheString(){
		String _ret = "[";
		for(int i = 0; i < scheme.length-1; i ++){
			_ret += scheme[i]+",";
		}
		_ret += scheme[scheme.length-1]+"]";
		return _ret;
	}
	
	public String[] getScheme(){
		return scheme;
	}
	
	public String getPath(){
		return path;
	}
	
	/* <a b c d e>  join <w b v d x> = <a b c d e w v x> */
	public Table join(Table _t, String _newPath) throws IOException{
		Table _ret=null;
		/*  */
		ArrayList<String> newList = new ArrayList<String>();
		{
			for(String iStr : scheme){
				newList.add(iStr);
			}
			final String[] _scheme = _t.getScheme();
			for(int i = 0; i < _scheme.length; i ++){
				String iStr = _scheme[i];
				boolean exist = false;
				for(String jStr : newList){
					if(jStr.equals(iStr))	exist = true;
				}
				if(!exist) newList.add(iStr);
			}
		}

		String[] newScheme = new String[newList.size()];
		newList.toArray(newScheme);
		_ret = new Table(newScheme, _newPath);
		
		{
			System.out.print(this.scheString() + " join " + _t.scheString() + 
					         " = " + _ret.scheString() + "\n");
		}
		{
			multiJoin.joinMR(this, _t, _newPath);
		}
		return _ret;
	}
}


class mjGroupComparator extends WritableComparator {

	
	protected mjGroupComparator() {
		super(tupleKey.class);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable _tk1, WritableComparable _tk2){
		int _ret = 0;
		_ret = ((tupleKey)_tk1).noTagCompareTo((tupleKey)_tk2);
		return _ret;
	}
	
}
