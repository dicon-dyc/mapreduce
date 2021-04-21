package xuliehua_tongjiliuliang;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private Text outk = new Text();
    private FlowBean outv = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();

        //2.切割
        String[] split = line.split("\t");

        //3.抓取想要的数据
        String phone = split[1];
        String up = split[split.length-3];
        String down = split[split.length-2];

        //4.封装
        outk.set(phone);
        outv.setUpflow(Long.parseLong(up));
        outv.setDownflow(Long.parseLong(down));
        outv.setSumflow();

        context.write(outk,outv);
    }
}
