package secondorder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class secondorderpartitioner extends Partitioner<secondorderbean, IntWritable> {
    @Override
    public int getPartition(secondorderbean secondorderbean, IntWritable intWritable, int numPartitions) {
        return Math.abs(secondorderbean.getFirst())%numPartitions;
    }
}
