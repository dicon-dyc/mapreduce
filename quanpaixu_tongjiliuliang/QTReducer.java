package quanpaixu_tongjiliuliang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class QTReducer extends Reducer<QTBean, Text,Text,QTBean> {
    @Override
    protected void reduce(QTBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
