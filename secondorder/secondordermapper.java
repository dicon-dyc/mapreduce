package secondorder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class secondordermapper extends Mapper<LongWritable, Text,secondorderbean,IntWritable>{

    private secondorderbean outk = new secondorderbean();
    private IntWritable outv = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        outk.setFirst(Integer.parseInt(split[1]));
        outk.setSecond(Integer.parseInt(split[0]));
        outv.set(Integer.parseInt(split[0]));

        context.write(outk,outv);


    }
}
