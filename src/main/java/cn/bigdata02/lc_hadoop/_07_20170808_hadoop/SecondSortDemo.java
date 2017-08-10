package cn.bigdata02.lc_hadoop._07_20170808_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondSortDemo {

    //根据你的数据，来写你自己的序列化类，但是这里的逻辑是有两个排序约定
    static class SecondSortWritable implements WritableComparable<SecondSortWritable>{
        static {
            WritableComparator.define(SecondSortWritable.class,new Comparato());
        }

        String word;
        int num;

        public SecondSortWritable() {
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public int compareTo(SecondSortWritable o) {
            return 0;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeInt(num);
        }

        public void readFields(DataInput in) throws IOException {
            word = in.readUTF();
            num = in.readInt();
        }

        @Override
        public String toString() {
            return word+" "+num;
        }

        static class Comparato extends WritableComparator{
            int m;
            public Comparato() {
                super(SecondSortWritable.class,true);
            }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                SecondSortWritable sw1 = (SecondSortWritable)a;
                SecondSortWritable sw2 = (SecondSortWritable)b;
                //二次排序，也就是和自己的之前写的序列化类不同的地方就在这里，之前只有一个约定，现在是两个约定
                m = sw1.word.compareTo(sw2.word);
                if (m == 0){//当第一个比较结果为相同的时候，我们约定第二个排序规则
                    m  = -(sw1.num - sw2.num);
                }
                return m;
            }
        }



    }

    static class SencondSortMap extends Mapper<LongWritable,Text,SecondSortWritable,NullWritable>{

        String[] strs;
        SecondSortWritable sw = new SecondSortWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            strs = value.toString().split("\t");
            sw.setWord(strs[0]);
            sw.setNum(Integer.parseInt(strs[1]));
            context.write(sw,NullWritable.get());
        }
    }


    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondSortDemo.class);
        job.setMapperClass(SencondSortMap.class);
        job.setOutputKeyClass(SecondSortWritable.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\hadoop\\4.txt"));
        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);
    }





}
