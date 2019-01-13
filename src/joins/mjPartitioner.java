package joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class mjPartitioner implements Partitioner<tupleKey, tupleVal>{

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(tupleKey key, tupleVal value, int numPartitions) {
		// TODO Auto-generated method stub
		return ((key.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions);
	}


}
