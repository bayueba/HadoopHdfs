package cn.hadoop.mr_4test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yqy on 17-8-4.
 */
public class MemberDemo2 {

    static int count=0;

    static class MemberWritable implements WritableComparable<MemberDemo2.MemberWritable> {

        String member_id;//会员id
        String gender;//性别
        String ffp_tier;//会员级别

        int P1Y_Flight_Count;//第一年乘机次数
        int L1Y_Flight_Count;//第二年乘机次数
        long num=0;//数量
        String rate="0";//比例

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public String getFfp_tier() {
            return ffp_tier;
        }

        public void setFfp_tier(String ffp_tier) {
            this.ffp_tier = ffp_tier;
        }

        public String getMember_id() {
            return member_id;
        }

        public void setMember_id(String member_id) {
            this.member_id = member_id;
        }

        public int getP1Y_Flight_Count() {
            return P1Y_Flight_Count;
        }

        public void setP1Y_Flight_Count(int p1Y_Flight_Count) {
            P1Y_Flight_Count = p1Y_Flight_Count;
        }

        public int getL1Y_Flight_Count() {
            return L1Y_Flight_Count;
        }

        public void setL1Y_Flight_Count(int l1Y_Flight_Count) {
            L1Y_Flight_Count = l1Y_Flight_Count;
        }

        public long getNum() {
            return num;
        }

        public void setNum(long num) {
            this.num = num;
        }

        public String getRate() {
            return rate;
        }

        public void setRate(String rate) {
            this.rate = rate;
        }

        @Override
        public String toString() {
            return "人数："+num+"占比:"+rate;
        }

        @Override
        public int compareTo(MemberDemo2.MemberWritable o) {
            if(gender==null){
                if(ffp_tier==null){
                    return 1;
                }else{
                    return this.ffp_tier.compareTo(o.ffp_tier);
                }
            }
            int i = this.gender.compareTo(o.gender);
            if (i == 0) {
                i = this.ffp_tier.compareTo(o.ffp_tier);
            }

            return i;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(gender);
            dataOutput.writeUTF(ffp_tier);
            dataOutput.writeLong(num);
            dataOutput.writeUTF(rate);

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            gender=dataInput.readUTF();
            ffp_tier=dataInput.readUTF();
            num=dataInput.readLong();
            rate=dataInput.readUTF();
        }
    }

    static  class MemberMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        LongWritable longWritable=new LongWritable(1);
        Text text=new Text();
        String[] values;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String v=new String(value.getBytes(),0,value.getLength(),"gbk");
           // System.out.println(v);
            values = v.split(",");
           String P1Y_Flight_Count=values[48];
           String  L1Y_Flight_Count=values[49];
           try {
               int piy=Integer.parseInt(P1Y_Flight_Count);
                int liy= Integer.parseInt(L1Y_Flight_Count);
               if (piy< liy) {
                   text.set("正增长率");
               } else if (piy>liy) {
                   text.set("负增长率");
               }else{
                   text.set("平");
               }
               count++;
               context.write(text,longWritable);
           }catch (NumberFormatException e){

               e.printStackTrace();
               return;
           }
        }
    }
    static class MemberReducer  extends Reducer<Text,LongWritable,Text,MemberDemo2.MemberWritable >{

        MemberWritable memberWritable=new MemberWritable();
//        long num=0;
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long num=0;
            for (LongWritable longWritable:values){
                num =num+longWritable.get();
            }
            memberWritable.setNum(num);
            memberWritable.setRate(num/Double.parseDouble(count+"")+"");

            System.out.println("num:"+num+"count"+count);
            context.write(key,memberWritable);

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MemberDemo2.class);
        job.setMapperClass(MemberMapper.class);
        job.setReducerClass(MemberReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MemberWritable.class);

        //job.setNumReduceTasks(5);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job,new Path("/home/yqy/2.csv"));
        Path path = new Path("/home/yqy/data");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
