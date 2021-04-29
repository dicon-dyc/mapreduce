package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class join_mapper extends Mapper<LongWritable, Text,Text,Text> {

    private Text outk1 = new Text();
    private Text outv1 = new Text();
    private Text outk2 = new Text();
    private Text outv2 = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outk1.set(split[0]);
        outv1.set("-"+split[1]);
        outk2.set(split[1]);
        outv2.set(split[0]);
        context.write(outk1,outv1);
        context.write(outk2,outv2);
    }
}
