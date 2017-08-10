package cn.bigdata02.lc_hadoop._07_20170808_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MulMRDemo {
    //第一个Mapper
    static class MulMap01 extends Mapper<LongWritable,Text,Text,LongWritable>{
        Text word = new Text();
        LongWritable number = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String val:value.toString().split(" ")){
                word.set(val);
                context.write(word,number);
            }
        }
    }
    //第一个reducer
    static class MulReduce01 extends Reducer<Text,LongWritable,Text,LongWritable>{
        LongWritable sumer = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val:values) {
                sum+=val.get();
            }
            sumer.set(sum);
            context.write(key,sumer);
        }
    }
    //第二个Mapper，输出变成LongWriteable，Text是为了对单词数量排序
    static class MulMap02 extends Mapper<LongWritable,Text,LongWritable,Text>{
        Text text = new Text();
        LongWritable number = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split("\t");
            number.set(Long.parseLong(val[1]));
            text.set(val[0]);

            context.write(number,text);
        }
    }
    //第二个rducer。
    static class MulReduce02 extends Reducer<LongWritable,Text,Text,LongWritable>{

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val:values ) {
                context.write(val,key);
            }
        }
    }
    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job01 = Job.getInstance(conf);//定义第一个job

        job01.setJarByClass(MulMRDemo.class);
        job01.setMapperClass(MulMap01.class);
        job01.setReducerClass(MulReduce01.class);

        job01.setOutputKeyClass(Text.class);
        job01.setOutputValueClass(LongWritable.class);

        ControlledJob cjob01 = new ControlledJob(conf);//需要将你的第一个job放入ControlledJob
        cjob01.setJob(job01);

        Path path01 = new Path("G:\\en");
        path01.getFileSystem(conf).delete(path01,true);
        FileInputFormat.addInputPath(job01,new Path("C:\\Users\\lc\\Desktop\\课件\\hadoop\\wordcount.txt"));
        FileOutputFormat.setOutputPath(job01,path01);

        Job job02 = Job.getInstance(conf);//开始配置第二个job信息
        job02.setJarByClass(MulMRDemo.class);
        job02.setMapperClass(MulMap02.class);
        job02.setReducerClass(MulReduce02.class);

        job02.setMapOutputKeyClass(LongWritable.class);
        job02.setMapOutputValueClass(Text.class);

        job02.setOutputKeyClass(Text.class);
        job02.setOutputValueClass(LongWritable.class);

        Path path02 = new Path("G:\\en02");
        path02.getFileSystem(conf).delete(path02,true);

        FileInputFormat.addInputPath(job02,new Path("G:\\en\\part*"));
        FileOutputFormat.setOutputPath(job02,path02);

        ControlledJob cjob02 = new ControlledJob(conf);//将你的第二个job放入ControlledJob
        cjob02.setJob(job02);
        cjob02.addDependingJob(cjob01);//设置依赖，也就说job02依赖job01

        JobControl jc = new JobControl("myjob");//设置总控制器，将第一个ControlledJob和第二个ControlledJob
                                                                //放入总控制器中
        jc.addJob(cjob01);
        jc.addJob(cjob02);

        Thread t = new Thread(jc);//必须将你的总控制器开启一个线程
        t.start();

        while (true){
            if(jc.allFinished()) {
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                break;
        }

        }





    }



}
