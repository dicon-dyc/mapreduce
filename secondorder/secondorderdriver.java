package secondorder;

import fenqu_tongjiliuliang.FTPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class secondorderdriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(secondorderdriver.class);

        job.setMapperClass(secondordermapper.class);
        job.setReducerClass(secondorderreducer.class);

        job.setMapOutputKeyClass(secondorderbean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(secondorderpartitioner.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\ij\\mapreduce\\src\\main\\input\\secondorder"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\ij\\mapreduce\\src\\main\\output\\secondorderoutput"));

        job.waitForCompletion(true);

    }
}
