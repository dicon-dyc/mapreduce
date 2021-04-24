package quanpaixu_tongjiliuliang;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class QTMapper extends Mapper<LongWritable,Text,QTBean,Text> {
    private QTBean outk = new QTBean();
    private Text outv = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outk.setUpflow(Long.parseLong(split[1]));
        outk.setDownflow(Long.parseLong(split[2]));
        outk.setSumflow();
        outv.set(split[0]);

        context.write(outk,outv);
    }
}
