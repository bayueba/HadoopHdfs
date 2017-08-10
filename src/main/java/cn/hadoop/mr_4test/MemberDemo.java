package cn.hadoop.mr_4test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 */
public class MemberDemo {
    static long count;
    static class MemberWritable implements WritableComparable<MemberWritable>{

        String gender;//性别
        String ffp_tier;//会员级别
        long num=0;//数量
        String rate="0";//比例
        long count;//总条数

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

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "MemberWritable{" +
                    "gender='" + gender + '\'' +
                    ", ffp_tier='" + ffp_tier + '\'' +
                    ", num=" + num +
                    ", rate='" + rate + '\'' +
                    ", count=" + count +
                    '}';
        }

        @Override
        public int compareTo(MemberWritable o) {
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
            num= dataInput.readLong();
            rate=dataInput.readUTF();

        }

    }

    static class MemberMapper extends Mapper<LongWritable,Text,MemberWritable,LongWritable>{
        MemberWritable memberWritable =new MemberWritable();
        LongWritable longWritable=new LongWritable(1);
        String[] values;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text=new String(value.getBytes(),0,value.getLength(),"gbk");
            System.out.println(text);

             values = text.split(",");

             String gender=values[3];//性别
             String ffp_tier=values[4];//会员级别
            System.out.println(gender+"===" +ffp_tier);
            if("男".equals(gender)||"女".equals(gender)){
                count++;
            }

            memberWritable.setGender(gender);
            memberWritable.setFfp_tier(ffp_tier);

            context.write(memberWritable,longWritable);
        }
    }
    static class MemberPartitioner extends Partitioner<MemberWritable,LongWritable> {

        static HashMap<String,Integer> map = new HashMap<String,Integer>();
        static {
            map.put("男6",0);
            map.put("女6",1);
            map.put("男5",2);
            map.put("女5",3);
        }
        String key;
        @Override
        public int getPartition(MemberWritable memberWritable, LongWritable longWritable, int i) {
            key=memberWritable.getGender()+memberWritable.getFfp_tier();
            Integer integer = map.get(key);
            if(null ==integer ||"".equals(integer)){
                integer=4;
            }
            return integer;
        }
    }

    static class MemberReducer extends Reducer<MemberWritable,LongWritable,MemberWritable,NullWritable>{

        MemberWritable memberWritable=new MemberWritable();
        long num;
        @Override
        protected void reduce(MemberWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            for (LongWritable longWritable:values){
                num+=longWritable.get();
            }
            memberWritable.setGender(key.getGender());
            memberWritable.setFfp_tier(key.getFfp_tier());
            memberWritable.setNum(num);
            memberWritable.setCount(count);
            //Double.parseDouble(num+"");
            //Double.parseDouble(count+"");
            memberWritable.setRate(Double.parseDouble(num+"")/Double.parseDouble(count+"")+"");

            context.write(memberWritable,NullWritable.get());

        }
    }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);

            job.setJarByClass(MemberDemo.class);
            job.setMapperClass(MemberMapper.class);
            job.setReducerClass(MemberReducer.class);
            job.setPartitionerClass(MemberPartitioner.class);//设置你自己的分区类

            job.setMapOutputKeyClass(MemberWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(MemberWritable.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(5);//设置你的reduceTask数量，有几个就会生出几个文件

            FileInputFormat.addInputPath(job,new Path("/home/yqy/2.csv"));
            Path path = new Path("/home/yqy/data");
            path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹

            FileOutputFormat.setOutputPath(job,path);

            System.exit(job.waitForCompletion(true)?0:1);
        }

}
