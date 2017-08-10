package mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yqy on 17-8-2.
 *  输入的kin和vin是来自于map的输出也就是kout和vout，但是呢
 * 在reduce这边过来的数据是kin和list(v1,v2....)这么结构
 *
 *
 */
public class WordCountReducer  extends Reducer<Text,LongWritable,Text,LongWritable> {

    //创建long类型的序列花类对象
    LongWritable longWritable = new LongWritable();
    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        //和
        long sum=0;
        //便利迭代values集和
        for (LongWritable val:values){
            //获取每个LongWritable值，相加
            sum+= val.get();
        }
        //给Vout赋值
        longWritable.set(sum);
        //写入环形缓存区,单词，总共出现次数
        context.write(key,longWritable);


    }

}