package quanpaixu_tongjiliuliang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class QTPartioner extends Partitioner<QTBean, Text> {

    @Override
    public int getPartition(QTBean qtBean, Text text, int numPartitions) {
        String line = text.toString();
        String substring = line.substring(0, 3);
        if ("136".equals(substring)){
            return 0;
        }else if ("137".equals(substring)){
            return 1;
        }else if ("138".equals(substring)){
            return 2;
        }else if ("139".equals(substring)){
            return 3;
        }else{
            return 4;
        }
    }
}
