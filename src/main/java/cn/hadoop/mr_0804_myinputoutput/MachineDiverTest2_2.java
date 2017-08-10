package cn.hadoop.mr_0804_myinputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yqy on 17-8-7.
 */
public class MachineDiverTest2_2 {

    static int count=0;
    static class MachineWritale implements WritableComparable<MachineWritale> {



        String reason;//原因
        long num;//数量

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        public long getNum() {
            return num;
        }

        public void setNum(long num) {
            this.num = num;
        }

        @Override
        public int compareTo(MachineWritale o) {
            int  i=(int)(o.num-this.num);
            return i;
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeUTF(reason);
            dataOutput.writeLong(num);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

            reason =dataInput.readUTF();
            num= dataInput.readLong();

        }

        @Override
        public String toString() {
            return reason+num;
        }
    }

    /**
     * Mapper
     */
    static class MyMapper extends Mapper<LongWritable,Text,MachineWritale,MachineWritale> {

        MachineWritale machineWritale=new MachineWritale();
        String[] values;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            values = value.toString().split("\t");
            if("".equals(values[0])||"".equals(values[1])){
                System.out.println("无效数据："+value);
                return ;
            }
            machineWritale.setReason( values[0]);
            machineWritale.setNum(Long.parseLong(values[1]));

            context.write(machineWritale,machineWritale);

        }
    }

    static class MachineReducer extends Reducer<MachineWritale,MachineWritale,NullWritable,MachineWritale> {


        @Override
        protected void reduce(MachineWritale key, Iterable<MachineWritale> values, Context context) throws IOException, InterruptedException {

            count++;
            if(count>=11){
                return;
            }

            context.write(NullWritable.get(),key);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MachineDiverTest2_2.class);
        //设置mapper的类
        job.setMapperClass(MyMapper.class);

        job.setReducerClass(MachineReducer.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(MachineWritale.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(MachineWritale.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MachineWritale.class);

        //输入的流的路径
        FileInputFormat.addInputPath(job,new Path("/home/yqy/data/bb/part-r-00000"));
        //路径
        Path path=new Path("/home/yqy/data/bb02");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
