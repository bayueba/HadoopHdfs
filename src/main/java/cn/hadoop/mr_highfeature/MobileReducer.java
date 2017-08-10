package cn.hadoop.mr_highfeature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class MobileReducer extends Reducer<MobileWritable,MobileWritable,Text,MobileWritable> {

    MobileWritable mobileWritable=new MobileWritable();
    Text text=new Text();
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
        text.set(key.getPhone());
        mobileWritable.setTraffic(traffic);
        context.write(text,mobileWritable);
    }
}

