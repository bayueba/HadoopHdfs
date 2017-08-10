package cn.hadoop.mr_0804_myinputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yqy on 17-8-5.
 */
public class MachineDiver {

    static class MachineWritale implements Writable {

        //使用时长
        long useLength;
        String useDate="";
        long num=1;

        public long getUseLength() {
            return useLength;
        }

        public void setUseLength(long useLength) {
            this.useLength = useLength;
        }

        public String getUseDate() {
            return useDate;
        }

        public void setUseDate(String useDate) {
            this.useDate = useDate;
        }

        public long getNum() {
            return num;
        }

        public void setNum(long num) {
            this.num = num;
        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeLong(useLength);
            dataOutput.writeUTF(useDate);
            dataOutput.writeLong(num);
        }
        @Override
        public void readFields(DataInput dataInput) throws IOException {

            useLength=dataInput.readLong();
            useDate=dataInput.readUTF();
            num=dataInput.readLong();
        }

        @Override
        public String toString() {
            return useDate;
        }
    }

    /**
     * map
     */
    static class MachineMapper extends Mapper<LongWritable,Text,Text,MachineWritale>{
        MachineWritale machineWritale=new MachineWritale();
        Text koutText=new Text();
        DateFormat df=new SimpleDateFormat("yyyy/mm/dd");
        String text;
        String[] values;
        String serviceType;
        String purchaseDate;
        String reservationDate;
        String brand;
        long useLength;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一行数据
            text=new String(value.getBytes(),0,value.getLength(),"utf-8");
            values = text.split(",");
            Logger logger= Logger.getLogger(this.getClass());
            if(values.length<25){
                logger.info("数据错误："+text);
                return;
            }
           // logger.info(text);
            //服务类型
            serviceType= values[24];
            if(key.get()==0){
                return;
            }


            if(!"维修".equals(serviceType)){
                logger.info("数据有问题："+text);
               return;
            }
            purchaseDate=values[0];//购机时间
            reservationDate=values[6];//预约时间
            brand =values[13];//品牌
            if(null==brand || "".equals(brand)){
                logger.info("数据有问题："+text);
                return ;
            }
            try {
                Date purchase = df.parse(purchaseDate);
                Date reservation = df.parse(reservationDate);
                 useLength=reservation.getTime()-purchase.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
                logger.info("数据有问题："+text);
                return;
            }
            machineWritale.setUseLength(useLength);
            koutText.set(brand);
            context.write(koutText,machineWritale);

        }
    }

    static class MachineReducer extends Reducer<Text,MachineWritale,Text,MachineWritale>{
        long data;
        MachineWritale machine=new MachineWritale();
        @Override
        protected void reduce(Text key, Iterable<MachineWritale> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            long useLength=0;
           for(MachineWritale machineWritale:values){
               sum +=  machineWritale.getNum();
              useLength+= machineWritale.getUseLength();
            }
           data= useLength/sum/(1000*3600*24);
            machine.setUseDate(data+"");
            context.write(key,machine);

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        //设置主类的class
        job.setJarByClass(MachineDiver.class);
        //设置mapper的类
        job.setMapperClass(MachineMapper.class);

        job.setReducerClass(MachineReducer.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(MachineWritale.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MachineWritale.class);

        //输入的流的路径
        FileInputFormat.addInputPath(job,new Path("/home/yqy/3.csv"));
        //路径
        Path path=new Path("/home/yqy/data/aa");
        //删除你的输出文件夹
        path.getFileSystem(configuration).delete(path,true);
        //结果输出到哪去
        FileOutputFormat.setOutputPath(job,path);
        //关闭
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
