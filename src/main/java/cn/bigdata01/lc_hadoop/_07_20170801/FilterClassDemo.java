package cn.bigdata01.lc_hadoop._07_20170801;

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

public class FilterClassDemo {
    /**
     * 自定义输出格式
     */
    static class FilterClassOutput extends FileOutputFormat<Text, LongWritable> {

        /**
         * @param job
         * @return
         * @throws IOException
         * @throws InterruptedException 重写getRecordWriter，需要返回一个RecordWriter
         */

        public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            //我们想返回一个自定义的RecordWriter-->FilterClassRecordWriter.FilterClassRecordWriterd的构造参数是
            //FSDataOutputStream fckey;
            //FSDataOutputStream fcvalue
            //这里我们围绕怎么得到这俩个流来做逻辑
            Configuration conf = job.getConfiguration();
            Path path01 = new Path("G:\\bigdata01\\1.txt");
            Path path02 = new Path("G:\\bigdata01\\2.txt");
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream fckey = fs.create(path01);
            FSDataOutputStream fcvalue = fs.create(path02);

            return new FilterClassRecordWriter(fckey,fcvalue);
        }
    }

    /**
     * 自定义RecordWriter，这里主要是规定输出文件放在哪
     */
    static class FilterClassRecordWriter extends RecordWriter<Text, LongWritable> {
        FSDataOutputStream fckey;
        FSDataOutputStream fcvalue;

        public FilterClassRecordWriter() {
        }

        /**
         * 上面两个流字段的构造方法，为了在FilterClassOutput实例化一个FilterClassRecordWriter
         *
         * @param fckey
         * @param fcvalue
         */
        public FilterClassRecordWriter(FSDataOutputStream fckey, FSDataOutputStream fcvalue) {
            this.fckey = fckey;
            this.fcvalue = fcvalue;
        }

        /**
         * 重写的write方法，数据来自context.write();
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, LongWritable value) throws IOException, InterruptedException {
            if (key.toString().split(",").length > 24) {
                fckey.writeUTF(key.toString() + " " + value.get() + "\n");
            } else {
                fcvalue.writeUTF(key.toString() + " " + value.get() + "\n");
            }
        }

        //关闭流，因为不关闭流，内容没有flush刷新。
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (fckey != null) {
                fckey.close();
            }
            if (fcvalue != null) {
                fcvalue.close();
            }
        }


    }

    /**
     * mapper类，这里什么都不做
     */
    static class FilterClassMap extends Mapper<LongWritable,Text,Text,LongWritable>{

        LongWritable number = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            number.set(value.toString().split(",").length);
            context.write(value,number);
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();//新建一个配置类实例

        Job job = Job.getInstance(conf);//获取一个Job实例
        /*
        之后的所有set是配置job信息
         */
        job.setJarByClass(FilterClassDemo.class);//设置jar的主类
        job.setMapperClass(FilterClassMap.class);//设置Mapper类

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path path = new Path("G:\\bigdata01demo");
        path.getFileSystem(conf).delete(path,true);//为了保证输出文件夹不存在

        job.setOutputFormatClass(FilterClassOutput.class);//设定自定义输出格式类

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\3.csv"));
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);



    }


}
