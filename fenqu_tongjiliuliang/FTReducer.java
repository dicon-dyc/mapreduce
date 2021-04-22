package fenqu_tongjiliuliang;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

public class FTReducer extends Reducer<Text,FTBean,Text, FTBean> {
    private FTBean outv = new FTBean();
    @Override
    protected void reduce(Text key, Iterable<FTBean> values, Context context) throws IOException, InterruptedException {
        long totleup = 0;
        long totledown = 0;
        for (FTBean value : values) {
            totleup += value.getUpflow();
            totledown += value.getDownflow();
        }
        outv.setUpflow(totleup);
        outv.setDownflow(totledown);
        outv.setSumflow();

        context.write(key,outv);
    }
}
