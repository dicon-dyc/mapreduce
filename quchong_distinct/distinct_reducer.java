package quchong_distinct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class distinct_reducer extends Reducer<Text,Text,Text,Text> {
    private Text outv = new Text("");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,outv);
    }
}
