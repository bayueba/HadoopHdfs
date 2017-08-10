package cn.hadoop.mr_highfeature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yqy on 17-8-3.
 * NullWritable
 */
public class MobileMapper extends Mapper<LongWritable,Text,MobileWritable,MobileWritable>{
    Logger logger = Log.getLogger().logger;
    String mobile;

    String[] values;
    String phone;
    String uploadTraffic;
    String downTraffic;
    MobileWritable mobileWritable=new MobileWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("MobileMapper--的map方法：");

        //\t是制表符

        mobile=value.toString().replaceAll("","");

        logger.info("切分后的mobile："+mobile);

        values = mobile.split("\t");
        int length = values.length;
        for (int i=0;i<values.length;i++){
            System.out.println("values"+values[i]);
        }
        System.out.println("多长："+length);
        phone= values[1];
        uploadTraffic = values[length-2];
        downTraffic = values[length-3];
        mobileWritable.setPhone(phone);
        mobileWritable.setUploadTraffic(Long.parseLong(uploadTraffic));
        mobileWritable.setDownTraffic(Long.parseLong(downTraffic));
        mobileWritable.setTraffic(Long.parseLong(uploadTraffic)+Long.parseLong(downTraffic));
        context.write(mobileWritable,mobileWritable);



    }
}
