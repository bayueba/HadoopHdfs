package mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <KIn,VIn,KOut,VOut>
 * 键，值    处理后的健，处理后的值
 * <p>
 * 一个块数据走一次这个类
 *
 * WordCount的mapper类。
 */
public class wordCount extends Mapper<LongWritable, Text, Text, LongWritable> {

    //创建Text对象
    Text text=new Text();
    //创建LongWritable对象，并设置值为1
    LongWritable longWritable=new LongWritable(1);
    String[] words;
    String str;
    /**
     * @param key 读的Key
     * @param value 读的数据
     * @param context 相当于环形缓存区
     * @throws IOException
     * @throws InterruptedException
     * 读一行数据走一次这个map方法
     *
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //根据条件替换
        str = value.toString().replaceAll(",|\\.|:", " ");
        System.out.println(str);
        //根据空格切割数据
        words = str.split(" ");
        //遍历单词数组
        for (String word:words) {
            //设置单词
            text.set(word);
            //写入环形缓存区（单词，出现一次）
            context.write(text,longWritable);
        }
    }

}
