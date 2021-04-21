package xuliehua_tongjiliuliang;

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outv = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        
        //1.遍历集合累加
        long totalUp = 0;
        long totalDown = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpflow();
            totalDown += value.getDownflow();
        }

        //2.封装
        outv.setUpflow(totalUp);
        outv.setDownflow(totalDown);

        context.write(key,outv);
    }
}
