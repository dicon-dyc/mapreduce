package fenqu_tongjiliuliang;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FTMapper extends Mapper<LongWritable, Text,Text, FTBean> {
    private FTBean outv = new FTBean();
    private Text outk = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");
        String phone = split[1];
        String up = split[split.length-3];
        String down = split[split.length-2];
        outv.setUpflow(Long.parseLong(up));
        outv.setDownflow(Long.parseLong(down));
        outv.setSumflow();
        outk.set(phone);
        context.write(outk,outv);
    }
}
