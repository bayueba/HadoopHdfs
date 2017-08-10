package cn.hadoop.mr_4partitiorer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 这个需求是根据手机号端来完成分区。那么首先自定义分区类
 */
public class BigData02Partition {

    //自定义分区类


    static class BigData02Part extends Partitioner<Text,NullWritable>{
        static HashMap<String,Integer> cc = new HashMap<String,Integer>();
        static {
            cc.put("133",0);
            cc.put("144",1);
            cc.put("166",2);
            cc.put("188",3);
        }
        Integer m;

        String key;
        public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {

            key = text.toString();
//            if (key.startsWith("133")){
//                numPartitions = 0;
//            }
//            else if (key.startsWith("166")){
//                numPartitions = 1;
//            }
//            else if (key.startsWith("188")){
//                numPartitions = 2;
//            }
//            else if (key.startsWith("144")){
//                numPartitions = 3;
//            }else{
//                numPartitions = 4;
//            }
            key = key.substring(0,3);
            System.out.println(key+"---->");
            m = cc.get(key);
            System.out.println(key+"---->");

            return m==null?4:m;
        }
    }

    static class BigData02PartitionMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        FileSystem fs;
        Text text = new Text();
        Path path = new Path("xxxx");
        String[] words;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fsin = fs.open(path);
            words = value.toString().split("\t");
            text.set(words[1]);
            context.write(text,NullWritable.get());
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(BigData02Partition.class);
        job.setMapperClass(BigData02PartitionMap.class);
        job.setPartitionerClass(BigData02Part.class);//设置你自己的分区类

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(5);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\hadoop\\mobileflow.log"));
        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);







    }






}


