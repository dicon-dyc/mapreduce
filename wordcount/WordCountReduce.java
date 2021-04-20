package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author : 若清 and wgh
 * Version : 2020/4/12 & 1.0
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    //实现父类方法快捷键  ctrl+o
    private IntWritable outv = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
                sum += value.get();
        }
        outv.set(sum);
        context.write(key,outv);
    }
}