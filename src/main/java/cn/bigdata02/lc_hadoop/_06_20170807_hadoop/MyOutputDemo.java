package cn.bigdata02.lc_hadoop._06_20170807_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputDemo {

    static class MyOutputPartition extends Partitioner<Text,NullWritable>{

        public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
            if (text.toString().contains("133")){
                return 1;
            }else {
                return 0;
            }
        }
    }
    /**
     * 自定义输出格式，需要继承FileOutputFormat，主要重写一个getRecordWriter方法。这里面是有些逻辑的
     */
    static class MyOutputFormat extends FileOutputFormat<Text,NullWritable>{
        Configuration conf;
        FileSystem fs;

        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            conf = job.getConfiguration();
            fs = FileSystem.get(conf);
            Path path = new Path("/home/yqy/data/20170807_06_01_0");
            Path path1 = new Path("/home/yqy/data/20170807_06_01_1");
            FSDataOutputStream fos = fs.create(path);
            FSDataOutputStream fos1 = fs.create(path1); //这个输出流就是给MyRecordWriter的流，让它往这个流里面写内容

            return new MyRecordWriter(fos,fos1,job);
        }
    }

    /**
     * 我的需求是，想实现个性化输出文件名。分析的结果是，要得到一个Path来开启一个FSDataOutputStream
     */
    static class MyRecordWriter extends RecordWriter<Text,NullWritable>{

        FSDataOutputStream fos;
        FSDataOutputStream fos1;
        TaskAttemptContext job;
        String str;

        public MyRecordWriter(FSDataOutputStream fos,FSDataOutputStream fos1,TaskAttemptContext job) {
            this.fos = fos;
            this.fos1 = fos1;
            this.job = job;
        }

        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            str = job.getTaskAttemptID().getTaskID().toString();
            System.out.println(str);

            if (str.endsWith("000")){
                System.out.println(str+"----->000");
                System.out.println(key.toString()+"key000");
                fos1.writeBytes(key.toString()+"\n");

            }
            if (str.endsWith("001")){
                System.out.println(str+"----->");
                System.out.println(key.toString()+"key");
                fos.writeBytes(key.toString()+"\n");//fos来自于构造器，这个构造器在自定义的FileOutputFormat中已经给定了
            }


        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (fos1!=null){
                fos1.close();
                System.out.println("fos1  close");
            }
            if (fos!=null){
                fos.close();
                System.out.println("fos  close");
            }


        }
    }

    static class MyOutputMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    public static void main(String[] agrs)throws Exception{


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MyOutputDemo.class);
        job.setMapperClass(MyOutputMap.class);
       // job.setPartitionerClass(MyOutputPartition.class);//设置你自己的分区类

        job.setOutputFormatClass(MyOutputFormat.class);//指定自己的输出格式类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //job.setNumReduceTasks(2);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job,new Path("/home/yqy/data/mobileflow.log"));
        Path path = new Path("/home/yqy/data/20170807_06_01");//但是，还是需要自定义输出文件夹。因为要有一个_SUCCESS文件要放
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);






    }




}
