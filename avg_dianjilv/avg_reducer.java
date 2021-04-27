package avg_dianjilv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class avg_reducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable outv = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long num = 0;
        for (LongWritable value : values) {
            sum += value.get();
            num+=1;
        }
        outv.set(sum/num);
        context.write(key,outv);
    }
}
