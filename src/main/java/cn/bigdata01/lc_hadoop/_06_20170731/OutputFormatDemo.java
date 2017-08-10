package cn.bigdata01.lc_hadoop._06_20170731;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputFormatDemo {

    static class MyRecordWriter extends RecordWriter<Text,LongWritable>{

        FSDataOutputStream fdosStr;
        FSDataOutputStream fdosNum;
        public MyRecordWriter() {
        }

        public MyRecordWriter(FSDataOutputStream fdosStr, FSDataOutputStream fdosNum) {
            this.fdosStr = fdosStr;
            this.fdosNum = fdosNum;
        }

        public void write(Text key, LongWritable value) throws IOException, InterruptedException {
            if (key.toString().equals("hello")){
                System.out.println("hello-->"+key.toString());
                fdosStr.writeBytes(key.toString()+" "+value.get()+"\n");
            }
            if(key.toString().equals("java")){
                System.out.println("java-->"+key.toString());
                fdosNum.writeBytes(key.toString()+" "+value.get()+"\n");
            }
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(fdosStr!=null){
                fdosStr.close();
            }
            if (fdosNum!=null){
                fdosNum.close();
            }
        }
    }

    static class MyOutputFormat extends FileOutputFormat<Text,LongWritable>{

        public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path fdosStr = new Path("/home/yqy/data/20170807_2/a.txt");
            Path fdosNum = new Path("/home/yqy/data/20170807_2/b.txt");

            FSDataOutputStream fsStr = fs.create(fdosStr);
            FSDataOutputStream fsNum = fs.create(fdosNum);

            return new MyRecordWriter(fsStr,fsNum);
        }
    }

    static class MyOutputFormatMap extends Mapper<LongWritable,Text,Text,LongWritable>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,new LongWritable(1));
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OutputFormatDemo.class);
        job.setMapperClass(MyOutputFormatMap.class);
        job.setOutputFormatClass(MyOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job,new Path("/home/yqy/a.txt"));
        Path path = new Path("/home/yqy/data/20170807_06/01");
        path.getFileSystem(conf).delete(path,true);
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);


    }







}
