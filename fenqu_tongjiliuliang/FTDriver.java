package fenqu_tongjiliuliang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class FTDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.jarbao
        job.setJarByClass(FTDriver.class);

        //3.连接mapper和reducer
        job.setMapperClass(FTMapper.class);
        job.setReducerClass(FTReducer.class);

        //4.设置map输出kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FTBean.class);

        //5.设置最终输出kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FTBean.class);

        //6.设置文件输入输出路径
        FileInputFormat.setInputPaths(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));

        //7.提交作业
        job.waitForCompletion(true);

    }
}
