package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class join_driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //实例配置
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(join_driver.class);

        //连接map reduce
        job.setMapperClass(join_mapper.class);
        job.setReducerClass(join_reducer.class);

        //设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //格式化输出路径
        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path("F:\\ij\\mapreduce\\src\\main\\output\\joinoutput");
        if (fs.exists(output)){
            fs.delete(output);
        }

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("F:\\ij\\mapreduce\\src\\main\\input\\join"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\ij\\mapreduce\\src\\main\\output\\joinoutput"));

        //提交job
        job.waitForCompletion(true);
    }
}
