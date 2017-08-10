package cn.hadoop.mr_4test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yqy on 17-8-4.
 */
public class MemberDemo3 {

    static class MemberWritable implements WritableComparable<MemberDemo3.MemberWritable> {


        String member_id;//会员id
        String gender;//性别
        String ffp_tier;//会员级别

        int P1Y_Flight_Count;//第一年乘机次数
        int L1Y_Flight_Count;//第二年乘机次数

        String rate = "0";//比例

        public String getMember_id() {
            return member_id;
        }

        public void setMember_id(String member_id) {
            this.member_id = member_id;
        }

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

        public String getRate() {
            return rate;
        }

        public void setRate(String rate) {
            this.rate = rate;
        }

        @Override
        public int compareTo(MemberDemo3.MemberWritable o) {

            return this.member_id.compareTo(o.member_id);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeUTF(member_id);
            dataOutput.writeUTF(gender);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    static class MemberMapper extends Mapper<LongWritable, Text, MemberWritable, NullWritable> {

        MemberWritable memberWritable = new MemberWritable();
        String[] values;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String v = new String(value.getBytes(), 0, value.getLength(), "gbk");
            System.out.println(v);
            values = v.split(",");
            String member_id = values[0];

            String P1Y_Flight_Count = values[48];
            String L1Y_Flight_Count = values[49];
            try {
                int piy = Integer.parseInt(P1Y_Flight_Count);
                int liy = Integer.parseInt(L1Y_Flight_Count);
                if (piy < liy) {
                    memberWritable.setRate("正增长率");
                } else if (piy > liy) {
                    memberWritable.setRate("负增长率");
                } else {
                    memberWritable.setRate("平");
                }
                context.write(memberWritable, NullWritable.get());
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){

    }


}
