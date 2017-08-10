package cn.hadoop.mr_0804_myinputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class MachineDiverTest2 {

    static class MachineWritale implements WritableComparable<MachineWritale>{


        String reason;//原因

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        @Override
        public int compareTo(MachineWritale o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeUTF(reason);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

             reason =dataInput.readUTF();
        }
    }


    /**
     * map
     */
    static class MachineMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        LongWritable longWritable=new LongWritable(1);
        Text koutText=new Text();
        String text;
        String[] values;
        String serviceType;
        String reason;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一行数据
            text=new String(value.getBytes(),0,value.getLength(),"utf-8");
            values = text.split(",");
            if(values.length<25){
                System.out.println("数据错误："+text);
                return;
            }
            //服务类型
            serviceType= values[24];
            if(key.get()==0){
                return;
            }
            if(!"维修".equals(serviceType)){
                System.out.println("不需要的数据："+text);
                return;
            }
            reason=values[23];//故障原因
            if("".equals(reason)){
                return ;
            }
            koutText.set(reason);

            context.write(koutText,longWritable);

        }
    }



    static class MachineReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        LongWritable longWritable=new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable longWritable:values){
                sum +=  longWritable.get();
            }

            longWritable.set(sum);
            context.write(key,longWritable);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MachineDiverTest2.class);
        //设置mapper的类
        job.setMapperClass(MachineMapper.class);

        job.setReducerClass(MachineReducer.class);
        //设置map输出的key类型
        //job.setMapOutputKeyClass(Text.class);
        //设置map输出的value类型
        //job.setMapOutputValueClass(MachineWritale.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //输入的流的路径
        FileInputFormat.addInputPath(job,new Path("/home/yqy/3.csv"));
        //路径
        Path path=new Path("/home/yqy/data/bb");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }

}
