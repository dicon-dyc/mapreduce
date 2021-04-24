package quanpaixu_tongjiliuliang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class QTDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(QTDriver.class);

        //连接map reduce
        job.setMapperClass(QTMapper.class);
        job.setReducerClass(QTReducer.class);

        //设置map输出kv类型
        job.setMapOutputKeyClass(QTBean.class);
        job.setMapOutputValueClass(Text.class);

        //设置输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(QTBean.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));

        //提交job
        job.waitForCompletion(true);
    }
}
