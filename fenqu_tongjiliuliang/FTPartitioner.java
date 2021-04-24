package fenqu_tongjiliuliang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FTPartitioner extends Partitioner<Text,FTBean> {

    @Override
    public int getPartition(Text text, FTBean ftBean, int numPartitions) {

        //获取一行
        String line = text.toString();

        //切片获取前三位
        String substring = line.substring(0, 3);

        //根据前缀判断返回值
        int partition;
        if ("136".equals(substring)){
            partition = 0;
        }else if ("137".equals(substring)){
            partition = 1;
        }else if ("138".equals(substring)){
            partition = 2;
        }else{
            partition = 3;
        }

        return partition;
    }
}
