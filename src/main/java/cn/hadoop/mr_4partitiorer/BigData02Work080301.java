package cn.hadoop.mr_4partitiorer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigData02Work080301 {

    static class BigData02Work080301Writable implements WritableComparable<BigData02Work080301Writable>{
        static long count = 0;
        String sex;
        String cardLevel;

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public String getCardLevel() {
            return cardLevel;
        }

        public void setCardLevel(String cardLevel) {
            this.cardLevel = cardLevel;
        }

        public int compareTo(BigData02Work080301Writable o) {
//            this.equals(o);

            return (this.sex.equals(o.sex)&&this.cardLevel.equals(o.cardLevel))?0:1;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(sex);
            out.writeUTF(cardLevel);
        }

        public void readFields(DataInput in) throws IOException {
            sex = in.readUTF();
            cardLevel = in.readUTF();
        }

        @Override
        public String toString() {
            return sex+cardLevel;
        }
    }

    static class BigData02Work080301Part extends Partitioner<BigData02Work080301Writable,LongWritable>{

        int m;
        public int getPartition(BigData02Work080301Writable bww, LongWritable longWritable, int numPartitions) {

            if ((bww.sex+bww.cardLevel).equals("男6")){
                m = 0;
            }else if ((bww.sex+bww.cardLevel).equals("女6")){
                m = 1;
            }else if ((bww.sex+bww.cardLevel).equals("男5")){
                m = 2;
            }else if ((bww.sex+bww.cardLevel).equals("女5")){
                m = 3;
            }else {
                m = 4;
            }

            return m;
        }
    }

    static class BigData02Work080301Map extends Mapper<LongWritable,Text,BigData02Work080301Writable,LongWritable>{
        String[] words;
        BigData02Work080301Writable bww = new BigData02Work080301Writable();
        LongWritable lw = new LongWritable(1);
        String str;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            str = new String(value.getBytes(),0,value.getLength(),"gbk");
//            System.out.println(key.get());
            if (key.get()==0){
                System.out.println(value.toString());
                return;//抛弃第一行数据，
            }
            words = str.split(",");
            if (words[3]==null || words[4]==null){
                return;//抛弃掉不知道存不存在的空指针
            }

                bww.setSex(words[3]);
                bww.setCardLevel(words[4]);
                bww.count++;
                context.write(bww,lw);

        }
    }

    static class BigData02Work080301Reduce extends Reducer<BigData02Work080301Writable,LongWritable,Text,FloatWritable>{
        long count;
        long sum;
        float result;
        Text text = new Text();
        FloatWritable fw = new FloatWritable();
        @Override
        protected void reduce(BigData02Work080301Writable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            count = BigData02Work080301Writable.count;
            sum = 0;
            for (LongWritable val:values){
                sum+=val.get();
            }
            result = (float)sum/count;
            text.set(key.sex+key.cardLevel+" "+sum+" "+count);
            fw.set(result);
            context.write(text,fw);
        }
    }

    public static void main(String[] agrs)throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(BigData02Work080301.class);
        job.setMapperClass(BigData02Work080301Map.class);
        job.setReducerClass(BigData02Work080301Reduce.class);
        job.setPartitionerClass(BigData02Work080301Part.class);//设置你自己的分区类

        job.setMapOutputKeyClass(BigData02Work080301Writable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setNumReduceTasks(5);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\2.csv"));
        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);



    }





}
