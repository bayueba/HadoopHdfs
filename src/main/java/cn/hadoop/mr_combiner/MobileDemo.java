package cn.hadoop.mr_combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MobileDemo {

    //需求是求和并排序吧，并且呢要使用自定义序列化
    static class BigData02Mobile implements WritableComparable<BigData02Mobile>{

        String phoneNumber;
        long sumLoad;

//        public BigData02Mobile() {
//        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public long getSumLoad() {
            return sumLoad;
        }

        public void setPhoneNumber(String phoneNumber) {
            this.phoneNumber = phoneNumber;
        }

        public void setSumLoad(long sumLoad) {
            this.sumLoad = sumLoad;
        }

        public int compareTo(BigData02Mobile o) {

            return this.phoneNumber.compareTo(o.phoneNumber);
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(phoneNumber);
            out.writeLong(sumLoad);
        }

        public void readFields(DataInput in) throws IOException {
            phoneNumber = in.readUTF();
            sumLoad = in.readLong();
        }

        @Override
        public String toString() {
            return phoneNumber+" "+sumLoad;
        }
    }

    static class BigData02WritableMap extends Mapper<LongWritable,Text,BigData02Mobile,BigData02Mobile>{

        String[] words;
        String phoneNumber;
        long sumLoad;
        BigData02Mobile bdm = new BigData02Mobile();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            words = value.toString().split("\t");
            phoneNumber = words[1];
            sumLoad = Long.parseLong(words[words.length-2])+Long.parseLong(words[words.length-3]);
            bdm.setPhoneNumber(phoneNumber);
            bdm.setSumLoad(sumLoad);
            context.write(bdm,bdm);
        }
    }

    /**
     * 注意，这个类是定义的Combiner类，主要作用是本地reduce，需要要特别注意的是，输入的K，V和输出的K,V必须一致
     */
    static class BigData02WritableCombiner extends Reducer<BigData02Mobile,BigData02Mobile,BigData02Mobile,BigData02Mobile>{
        BigData02Mobile bdm = new BigData02Mobile();
        @Override
        protected void reduce(BigData02Mobile key, Iterable<BigData02Mobile> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (BigData02Mobile val:values) {
                sum+=val.getSumLoad();
            }
            bdm.setPhoneNumber(key.phoneNumber);
            bdm.setSumLoad(sum);
            context.write(bdm,bdm);
        }



    }

    static class BigData02WritbalReduce extends Reducer<BigData02Mobile,BigData02Mobile,Text,LongWritable>{
        Text text = new Text();
        LongWritable lw = new LongWritable();
        @Override
        protected void reduce(BigData02Mobile key, Iterable<BigData02Mobile> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (BigData02Mobile val:values) {
                sum+=val.getSumLoad();
            }
            lw.set(sum);
            text.set(key.getPhoneNumber());
            context.write(text,lw);
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(MobileDemo.class);
        job.setMapperClass(BigData02WritableMap.class);
        job.setReducerClass(BigData02WritbalReduce.class);

        job.setMapOutputKeyClass(BigData02Mobile.class);
        job.setMapOutputValueClass(BigData02Mobile.class);

        job.setCombinerClass(BigData02WritableCombiner.class);//设置你的Combiner类

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job,new Path("/home/yqy/mobileflow.log"));
        Path path = new Path("/home/yqy/data");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);







    }





}
