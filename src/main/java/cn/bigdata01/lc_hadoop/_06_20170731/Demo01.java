package cn.bigdata01.lc_hadoop._06_20170731;

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
 * 取0--购买时间，6--预约时间，13--品牌，24--服务类型
 */

public class Demo01 {
    enum MyCounter{
        NUMBER,
        LESSTOW,
        ANZHUANG,
        NULL;
    }

    static class MachineMap extends Mapper<LongWritable,Text,Text,Text>{

        Text goods = new Text();
        Text str = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0){
                context.getCounter(MyCounter.NUMBER).increment(1);
                return;
            }
            String[] words = value.toString().split(",");
            if (words.length>24) {
                if (words[0] == null || words[6] == null || words[13] == null || words[24] == null) {
                    context.getCounter(MyCounter.NULL).increment(1);
                    return;
                }
                if (words[24].equals("维修")) {
                    goods.set(words[13]);
//                    System.out.println(words[0] + " " + words[6] + " " + 1);
                    str.set(words[0] + " " + words[6] + " " + 1);
                    context.write(goods, str);
                }else{
                    context.getCounter(MyCounter.ANZHUANG).increment(1);
                }
            }else {
                context.getCounter(MyCounter.LESSTOW).increment(1);
            }
        }
    }

    static class MachineReduce extends Reducer<Text,Text,Text,LongWritable>{
        String[] words;
        String[] buyTime;
        String[] badTime;
        LongWritable lw = new LongWritable(1);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long year = 0;
            long month = 0;
            long day = 0;
            for (Text val:values){
                words = val.toString().split(" ");
                buyTime = words[0].split("/");
                badTime = words[1].split("/");
//                System.out.println(buyTime[0]+buyTime[1]+buyTime[2]);
//                System.out.println(badTime[0]+badTime[1]+badTime[2]+"--->");
                if (buyTime.length==3 && badTime.length==3){
                    try{
                        sum+=Long.parseLong(words[2]);
                        year+=(Long.parseLong(badTime[0])-Long.parseLong(buyTime[0]));
                        month+=(Long.parseLong(badTime[1])-Long.parseLong(buyTime[1]));
                        day+=(Long.parseLong(badTime[2])-Long.parseLong(buyTime[2]));

                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }

            }
            System.out.println(key.toString()+(year*365+month*30+day)/sum);
            lw.set((long)(year*365+month*30+day)/sum);
            context.write(key,lw);
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Demo01.class);
        job.setMapperClass(MachineMap.class);
        job.setReducerClass(MachineReduce.class);

        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path path = new Path(agrs[1]);
        path.getFileSystem(conf).delete(path,true);
        FileInputFormat.addInputPath(job,new Path(agrs[0]));
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);


    }




}
