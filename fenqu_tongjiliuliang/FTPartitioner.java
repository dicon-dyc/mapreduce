package fenqu_tongjiliuliang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FTPartitioner extends Partitioner<Text,FTBean> {

    @Override
    public int getPartition(Text text, FTBean ftBean, int numPartitions) {

        String line = text.toString();
        String substring = line.substring(0, 3);
        int partition;
        if ("136".equals(substring)){
            partition = 1;
        }else if ("137".equals(substring)){
            partition = 2;
        }else if ("138".equals(substring)){
            partition = 3;
        }else{
            partition = 4;
        }

        return partition;
    }
}
