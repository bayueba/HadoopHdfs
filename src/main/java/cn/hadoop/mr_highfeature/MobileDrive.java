package cn.hadoop.mr_highfeature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
/**
 * Created by yqy on 17-8-3.
 */
public class MobileDrive {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        //根据conf来获得一个Job的实例，通过这个Job来实现任务的设置
        Job job = Job.getInstance(configuration);
        //设置main主类Drive类
        job.setJarByClass(MobileDrive.class);
        //设置mapper类
        job.setMapperClass(MobileMapper.class);
        //设置reducer类
        job.setReducerClass(MobileReducer.class);

        //设置combiner类
        job.setCombinerClass(MobileCombiner.class);

        //若mapper的KOut的类型与Reducer输出的KOut类型不一致的话需设置
        job.setMapOutputKeyClass(MobileWritable.class);
        //若mapper输出的value值与reducer输出的value值不是一个类型需设置
        job.setMapOutputValueClass(MobileWritable.class);

        //设置输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置输出的value类型
        job.setOutputValueClass(MobileWritable.class);

        String p="/home/yqy/mobileflow.log";
        //添加输入的文件路径，main方法数组的第一个值，为对哪个文件进行处理文件路径
        FileInputFormat.addInputPath(job,new Path(p));
       // Path path = new Path(args[1]);
         Path path = new Path("/home/yqy/data");
        //删除存到哪的目录
        path.getFileSystem(configuration).delete(path,true);
        //设置这个结果输出到哪里去，，，这个目录必须不存在
        FileOutputFormat.setOutputPath(job,path);

        //如果处理成功，结果为true
        boolean b = job.waitForCompletion(true);
        //如果为true，结束程序
        System.exit(b?0:1);
    }
}
