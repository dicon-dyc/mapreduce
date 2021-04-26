package quchong_distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class distinct_driver{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //实例化配置文件
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(distinct_driver.class);

        //格式路径
        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path("F:\\ij\\mapreduce\\src\\main\\output\\quchong_distinctoutput");
        if (fs.exists(output)){
            fs.delete(output,true);
        }

        //连接map reduce
        job.setMapperClass(distinct_mapper.class);
        job.setReducerClass(distinct_reducer.class);

        //设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输出输出路径
        FileInputFormat.setInputPaths(job,new Path("F:\\ij\\mapreduce\\src\\main\\input\\quchong_distinctinput"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\ij\\mapreduce\\src\\main\\output\\quchong_distinctoutput"));

        //提交job
        job.waitForCompletion(true);
    }
}
