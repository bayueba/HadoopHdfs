package cn.hadoop.mr_highfeature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * 需求
 *实现序列花的接口
 */
public class MobileWritable implements WritableComparable<MobileWritable> {

    //手机号
    private String phone;
    //上传的流量
    private long uploadTraffic;
    //下载的流量
    private long downTraffic;
    private long traffic;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUploadTraffic() {
        return uploadTraffic;
    }

    public void setUploadTraffic(long uploadTraffic) {
        this.uploadTraffic = uploadTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }

    public long getTraffic() {
        return traffic;
    }

    public void setTraffic(long traffic) {
        this.traffic = traffic;
    }

    //如果将自己的序列化类当做key，那么你必须要将无参空构造显式的写出来
    public MobileWritable() {

    }


    /**
     * 比较器排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(MobileWritable o) {
       return phone.compareTo(o.getPhone());
    }

    //必须要写的序列化和反序列化方法，注意你的序列化和反序列化的字段顺序必须一致
    /**
     * 序列花
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //设置序列花的字段
        dataOutput.writeUTF(phone);//手机号
        dataOutput.writeLong(uploadTraffic);//上传的流量
        dataOutput.writeLong(downTraffic);
        dataOutput.writeLong(traffic);
    }

    /**
     * 反序列
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        phone=dataInput.readUTF();
        uploadTraffic=dataInput.readLong();
        downTraffic=dataInput.readLong();
        traffic= dataInput.readLong();


    }

    @Override
    public String toString() {
        return "traffic=" + traffic ;
    }
}
