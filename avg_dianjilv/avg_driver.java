package avg_dianjilv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class avg_driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //实例化配置文件
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(avg_driver.class);

        //清空文件输出路径
        FileSystem fs = FileSystem.get(configuration);
        Path out_file = new Path("F:\\ij\\mapreduce\\src\\main\\output\\avg_output");
        if (fs.exists(out_file)){
            fs.delete(out_file);
        }

        //连接maop reduce
        job.setMapperClass(avg_mapper.class);
        job.setReducerClass(avg_reducer.class);

        //设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("F:\\ij\\mapreduce\\src\\main\\input\\svg"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\ij\\mapreduce\\src\\main\\output\\avg_output"));

        //提交任务
        job.waitForCompletion(true);
    }
}
