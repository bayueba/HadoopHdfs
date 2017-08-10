package cn.bigdata02.lc_hadoop._06_20170807_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 它是Hadoopo的自带计数器，用来在log中显式一些信息。然而，计数器，对我们开发来说，用处不大，只是调理一些bug的时候
 * 可以知道某些细节。
 */
public class CounterDemo {
    /**
     * 想自定义一个计数器，需要自定义一个枚举，枚举和类是一样的，只是修饰符不同，类是用class来修饰的
     * 枚举用enum来修饰。
     */
    enum Mycounter{
        MAPCOUNT,
        REDUCECOUNT;
    }

    static class MyCounterMap extends Mapper<LongWritable,Text,Text,NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Mycounter. MAPCOUNT).increment(1);//increment方法是计数方法。它用来设定每次枚举几次。
            System.out.println(context.getCounter(Mycounter.MAPCOUNT).getValue());
            context.write(value, NullWritable.get());
        }
    }


    static class MyCounterReduce extends Reducer<Text,NullWritable,Text,NullWritable>{


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(context.getCounter(Mycounter.MAPCOUNT).getValue()+"--->reduce");//很遗憾的实验，发现枚举并不能在mapper方法中计数，而在Reducer这边取出。
            //所以说，counter对开发来说意义不大
            context.write(key,values.iterator().next());
        }
    }

    public static void main(String[] agrs)throws Exception{



        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CounterDemo.class);
        job.setMapperClass(MyCounterMap.class);
        job.setReducerClass(MyCounterReduce.class);
//        job.setPartitionerClass(BigData02Partition.BigData02Part.class);//设置你自己的分区类

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        job.setNumReduceTasks(5);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\hadoop\\mobileflow.log"));
        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);



    }









}
