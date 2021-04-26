package quchong_distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class distinct_mapper extends Mapper<LongWritable, Text,Text, Text> {
    private Text outk = new Text();
    private Text outv = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outk.set(split[1]);
        outv.set("");
        context.write(outk,outv);
    }
}
