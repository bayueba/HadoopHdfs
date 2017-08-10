package cn.bigdata01.lc_hadoop._06_20170731;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InputFormatDemo {
    /**
     * 自定义RecordReader
     */
    static class MyRecordReader extends RecordReader<Text,NullWritable>{
        FileSplit fileSplit;
        boolean flag = false;
        NullWritable none = NullWritable.get();
        Text pathName = new Text() ;
        Configuration conf;
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            fileSplit = (FileSplit)split;
            conf = context.getConfiguration();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(!flag){
                pathName.set(fileSplit.getPath().getName());
                flag = true;
                return true;
            }
            return false;
        }

        public Text getCurrentKey() throws IOException, InterruptedException {
            return pathName;
        }

        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return none;
        }

        public float getProgress() throws IOException, InterruptedException {
            return flag?0.0f:1.0f;
        }

        public void close() throws IOException {

        }
    }

    /**
     * 自定义FileInputFormat
     */
    static class MyFileInputFormat extends FileInputFormat<Text,NullWritable>{

//        @Override
//        protected boolean isSplitable(JobContext context, Path filename) {
//            return false;
//        }

        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            MyRecordReader mrr = new MyRecordReader();
            mrr.initialize(split,context);
            return mrr;
        }
    }

    static class MyFileInputMap extends Mapper<Text,NullWritable,Text,NullWritable>{
        enum MyEnum{
            NUMBER;
        }
        @Override
        protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
            context.getCounter(MyEnum.NUMBER).increment(1);
        }
    }

    public static void main(String[] agrs)throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InputFormatDemo.class);
        job.setInputFormatClass(MyFileInputFormat.class);
        job.setMapperClass(MyFileInputMap.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\lc\\Desktop\\课件\\hadoop\\配置文件"));
        Path path = new Path("G:\\en");
        path.getFileSystem(conf).delete(path,true);
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);



    }










}
