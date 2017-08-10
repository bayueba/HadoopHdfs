package cn.hadoop.mr_0807_myoutput;

import javafx.concurrent.Task;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yqy on 17-8-7.
 */
public class MyFileOutputFormat {


    /**
     * Mapper
     */
    static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }

    /**
     * 自定义输出格式，需要继承FileOutputFormat抽象类，主要重写一个getRecordWriter方法。这里面是有些逻辑的
     */
    static class MyOutputFormat  extends FileOutputFormat<Text,LongWritable> {

        /**
         * 重写getecordriter方法
         * @param taskAttemptContext
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            //获取配置的Configuration对象
            Configuration configuration = taskAttemptContext.getConfiguration();
            //获取文件系统的对象
            FileSystem fileSystem = FileSystem.get(configuration);

            Path path=new Path ("/home/yqy/data/0808/输出流/");
            Path path2=new Path("/home/yqy/data/0808/输出流2/");
            //根据路径获取输出流的对象
            FSDataOutputStream fdos = fileSystem.create(path);
            FSDataOutputStream fdos2 = fileSystem.create(path);

            return new MyRecordWriter(fdos,fdos2,taskAttemptContext);
        }
    }

    /**
     * 我的需求是，想实现个性化输出文件名。分析的结果是，要得到一个Path来开启一个FSDataOutputStream
     */
    static class MyRecordWriter extends RecordWriter<Text,LongWritable> {

        FSDataOutputStream fdos;
        FSDataOutputStream fdos2;
        TaskAttemptContext job;
        public MyRecordWriter(){

        }

        public MyRecordWriter(FSDataOutputStream fdos,FSDataOutputStream fdos2,TaskAttemptContext job) {

            this.fdos=fdos;
            this.fdos2=fdos2;
            this.job=job;

        }


        @Override
        public void write(Text key, LongWritable value) throws IOException, InterruptedException {
            //获取
            String str = job.getTaskAttemptID().getTaskID().toString();
            System.out.println(str);

            if(key.toString().contains("hello")){
                System.out.println("hello-->"+key.toString());
                //fos来自于构造器，这个构造器在自定义的FileOutputFormat中已经给定了
                fdos.writeBytes(key.toString()+" "+value.get()+"\n");
            }else{
                System.out.println("其他》》》："+key.toString());
                fdos2.writeBytes(key.toString()+"=="+value.get()+"\n");
            }
        }

        /**
         *关闭流  关闭流，因为不关闭流，内容没有flush刷新。
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            if(null!=fdos){
                fdos.close();
            }
            if(null!=fdos2){
                fdos2.close();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MyFileOutputFormat.class);
        //设置mapper的类
        job.setMapperClass(MyMapper.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(LongWritable.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path("/home/yqy/2.csv"));
        //但是，还是需要自定义输出文件夹。因为要有一个_SUCCESS文件要放
        Path path=new Path("/home/yqy/data/b");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
