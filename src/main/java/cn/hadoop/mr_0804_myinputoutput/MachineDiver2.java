package cn.hadoop.mr_0804_myinputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by yqy on 17-8-5.
 */
public class MachineDiver2 {
    static class MachineWritale2 implements WritableComparable<MachineWritale2> {

        String brand;
        //使用时长
        long useLength;

        public long getUseLength() {
            return useLength;
        }

        public void setUseLength(long useLength) {
            this.useLength = useLength;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeLong(useLength);
            dataOutput.writeUTF(brand);
        }
        @Override
        public void readFields(DataInput dataInput) throws IOException {

            useLength=dataInput.readLong();
            brand=dataInput.readUTF();
        }

        @Override
        public String toString() {
            return brand+useLength;
        }

        @Override
        public int compareTo(MachineWritale2 o) {

           long a= o.useLength-this.useLength;
           return (int)a;
        }
    }
    static class MachineMapper2 extends Mapper<LongWritable,Text,MachineWritale2,MachineWritale2>{

        MachineWritale2 machineWritale2=new MachineWritale2();
        String[] values;
        String useLength;
        String brand;

       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

           System.out.println(value.toString());
            values = value.toString().split("\t");

           System.out.println(values[0]);
           System.out.println(values[1]);

            brand = values[0];
            useLength = values[values.length - 1];
            machineWritale2.setBrand(brand);
            machineWritale2.setUseLength(Long.parseLong(useLength));

            context.write(machineWritale2,machineWritale2);



        }
    }
    static class MachineReducer2 extends Reducer<MachineWritale2,MachineWritale2,NullWritable,MachineWritale2>{


        @Override
        protected void reduce(MachineWritale2 key, Iterable<MachineWritale2> values, Context context) throws IOException, InterruptedException {

            context.write(NullWritable.get(),key);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MachineDiver2.class);
        //设置mapper的类
        job.setMapperClass(MachineMapper2.class);

        job.setReducerClass(MachineReducer2.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(MachineWritale2.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(MachineWritale2.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MachineWritale2.class);

        //输入的流的路径
        FileInputFormat.addInputPath(job,new Path("/home/yqy/data/aa/part-r-00000"));
        //路径
        Path path=new Path("/home/yqy/data/aa02");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
