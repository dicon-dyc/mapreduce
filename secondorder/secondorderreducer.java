package secondorder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class secondorderreducer extends Reducer<secondorderbean, IntWritable, Text,Text> {

    private Text outk = new Text();
    private Text outV = new Text();

    @Override
    protected void reduce(secondorderbean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            outk.set(Integer.toString(key.getFirst()));
            outV.set(value.toString());
            context.write(outk,outV);
        }
    }
}
