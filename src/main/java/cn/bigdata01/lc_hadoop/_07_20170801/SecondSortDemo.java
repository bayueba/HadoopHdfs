package cn.bigdata01.lc_hadoop._07_20170801;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 二次排序的demo
 */
public class SecondSortDemo {

    //首先需要一个JavaBean，因为是二次排序，所以最低需要两个字段。
    static class WordSecondSort implements WritableComparable<WordSecondSort>{
        String word;
        long number;
        //如果我们自己的序列化类要当做key，也就是说需要排序，我们的序列化类的无参空构造需要显式声明
        //如果不显式声明，会抛空指针异常
        public WordSecondSort() {
        }

        public String getWord() {
            return word;
        }

        public long getNumber() {
            return number;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public void setNumber(long number) {
            this.number = number;
        }
        //如果没有二次排序的需求，这里我们只需要规定排序规则就可以了。
        //如果有二次排序需求，这里我们不仅要规定第一次的排序规则，还需要初步定义第二次排序的规则
        public int compareTo(WordSecondSort o) {
            int m = this.word.compareTo(o.word); //第一次约定排序规则
            if (m == 0){
                m = this.number>o.number?-1:1;//第二次约定排序规则
            }

            return m;
        }
        //重写序列化和反序列化的方法，注意，字段的序列化和反序列化过程要一致
        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeLong(number);
        }

        public void readFields(DataInput in) throws IOException {
            word = in.readUTF();
            number = in.readLong();
        }

        @Override
        public String toString() {
            return  word + " "+number;
        }
    }

    static class GroupSort extends WritableComparator{
        //这个类，主要是对分组中的数据进行排序,这里需要显式的无参空构造，通过反射机制得到一个javaBean实例
        //如果说没有这个东西，那么会报空指针异常
        public GroupSort() {
            super(WordSecondSort.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            WordSecondSort c = (WordSecondSort)a;
            WordSecondSort d = (WordSecondSort)b;
            return c.word.compareTo(d.word);
        }
    }

    static class SecondSortMap extends Mapper<LongWritable,Text,WordSecondSort,NullWritable>{
        WordSecondSort wss = new WordSecondSort();
        NullWritable none = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            wss.setWord(words[0]);
            wss.setNumber(Long.parseLong(words[1]));
            context.write(wss,none);
        }
    }

    static class SecondSortReduce extends Reducer<WordSecondSort,NullWritable,Text,NullWritable>{
        Text text = new Text();
//        NullWritable none = NullWritable.get();
        @Override
        protected void reduce(WordSecondSort key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable val:values) {
                text.set(key.toString());
                context.write(text,val);
            }
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondSortDemo.class);
        job.setMapperClass(SecondSortMap.class);
        job.setReducerClass(SecondSortReduce.class);

        job.setMapOutputKeyClass(WordSecondSort.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);

//        job.setGroupingComparatorClass(GroupSort.class);//设置分组排序的类
//
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\4.txt"));
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);




    }



}
