package cn.hadoop.mr_highfeature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 *   /**
 * 注意，这个类是定义的Combiner类，主要作用是本地reduce，需要要特别注意的是，输入的K，V和输出的K,V必须一致
 */
public class MobileCombiner extends Reducer<MobileWritable,MobileWritable,MobileWritable,MobileWritable> {
    MobileWritable mobileWritable=new MobileWritable();
    @Override
    protected void reduce(MobileWritable key, Iterable<MobileWritable> values, Context context) throws IOException, InterruptedException {

        long uploadTraffic=0;
        long downTraffic=0;
        long traffic=0;

        for (MobileWritable mo: values){

            uploadTraffic+=mo.getUploadTraffic();
            downTraffic+=mo.getDownTraffic();
            traffic +=mo.getTraffic();
        }
        mobileWritable.setPhone(key.getPhone());
        mobileWritable.setTraffic(traffic);
        context.write(mobileWritable,mobileWritable);
    }
}
