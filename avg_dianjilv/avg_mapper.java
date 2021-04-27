package avg_dianjilv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Long.parseLong;

public class avg_mapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    private Text outk = new Text();
    private LongWritable outv = new LongWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        outk.set(splits[0]);
        outv.set(Long.parseLong(splits[1]));
        context.write(outk,outv);

    }
}
