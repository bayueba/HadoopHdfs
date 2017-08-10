package cn.hadoop.mr_0804_myinputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by yqy on 17-8-4.
 */
public class MyFileInputFormat {

    /*
    自定义输入格式。需要继承FileInputFormat。里面主要是重写createRecordReader方法
    ，返回一个自定义的RecordReader
     */
    static class MyInputFormat extends FileInputFormat<NullWritable, Text> {


        @Override
        public RecordReader<NullWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordReader();
        }
    }


    /*
   自定义RecordReader，它是主要的处理输入数据格式的类，最终是写初始化方法和nextKeyValue
   那么，数据流程是块block-->Job--->InputFormat--->RecordReader-->Map
    */
   static class  MyRecordReader extends RecordReader<NullWritable,Text>{

        //因为我们想自己完成一行一行读，发现这个LineReader可以完成，所以初始化时，我们是围绕得到它的一个实例为目标
        LineReader lr;
        //读取后的value数据存放在此变量
        Text value=new Text();
        int len;
        FSDataInputStream fsDataInputStream;
        String text;
        /**
         * 初始化
         * @param inputSplit
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            //得到Configuration对象
            Configuration configuration = taskAttemptContext.getConfiguration();
            //得到操作文件系统的对象
            FileSystem fileSystem = FileSystem.get(configuration);
            //FileSplit是InputSplit的继承子类
            FileSplit fileSplit= (FileSplit)inputSplit;
            //获取输入的流的文件路径
            Path path = fileSplit.getPath();
            //根据文件路径获取输入流
             fsDataInputStream = fileSystem.open(path);
            //看API发现，想实例化一个LineReader最低要求是得到一个输入流，所以上面的一切操作都是为了得到输入流
            lr=new LineReader(fsDataInputStream);

        }

        /**
         * 对逻辑的处理 key value的
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            //读取一行数据到value变量中，返回值是这个变量的长度
            len= lr.readLine(value);
            //如过长度为0的时候说明没东西了
            System.out.println("长度:"+len);
            System.out.println(""+value.getLength());
            //csv,文件gedit打开，本生就是有问题的，所以通过字符串转了一下
            text=new String(value.getBytes(),0,value.getLength(),"GBK");
            value.set(text);
            if(len==0){
                //结束
                return false;
            }
            return true;
        }

        /**
         * 设置maper的key
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return true?0.0f:1.0f;
        }

        /**
         * 关闭流
         * @throws IOException
         */
        @Override
        public void close() throws IOException {

            if(fsDataInputStream!=null){
                fsDataInputStream.close();

            }
        }
    }

    /**
     * Mapper
     */
    static class MyMapper extends Mapper<NullWritable,Text,Text,NullWritable>{

        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MyFileInputFormat.class);
        //设置mapper的类
        job.setMapperClass(MyMapper.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(MyInputFormat.class); //设置自己的输入格式类

        //输入的流的路径
        FileInputFormat.addInputPath(job,new Path("/home/yqy/2.csv"));
        //路径
        Path path=new Path("/home/yqy/data/b");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }
}


