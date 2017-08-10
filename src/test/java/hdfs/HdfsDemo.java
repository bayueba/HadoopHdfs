package hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;


/**
 * Created by yqy on 17-8-1.
 */
public class HdfsDemo {
    FileSystem fs;

    /**
     * 初始化fs，得到一个HDFS的文件系统实例
     * @throws Exception
     */
    /**
     * 初始化fs，得到一个HDFS的文件系统实例
     * @throws Exception
     */
    @Before
    public void init()throws Exception{
        //读取配置文件，并解析其内容，封装到conf对象中
        Configuration conf = new Configuration();
        //hdfs文件系统的uri
        URI uri = new URI("hdfs://192.168.37.100:9000");
        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象
        fs = FileSystem.get(uri, conf,"root");
    }

    /**
     * 创建一个文件夹
     */
    @Test
    public void createMkdir()throws Exception{
        fs.mkdirs(new Path("/BigDataClassTwo"));
    }

    /**
     * 上传文件，比较底层的写法
     *
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {

       // Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://hadoop:9000/");
       // FileSystem fs = FileSystem.get(conf);

        //路径
        Path dst = new Path("hdfs://hadoop:9000/aa/aa.txt");
        //创建这个路径的输出流
        FSDataOutputStream os = fs.create(dst);
        //本地的输入流
        FileInputStream is = new FileInputStream("/home/yqy/aa.txt");
        //复制输入流到输出流
        IOUtils.copy(is, os);


    }

    /**
     * 上传文件，封装好的写法
     * @throws Exception
     * @throws IOException
     */
    @Test
    public void upload2() throws Exception, IOException{

        fs.copyFromLocalFile(new Path("/home/yqy/aa.txt"), new Path("hdfs://hadoop:9000/aaa/bbb/ccc/aa.txt"));

    }


    /**
     * 下载文件
     * @throws Exception
     * @throws IllegalArgumentException
     */
    @Test
    public void download() throws Exception {

        fs.copyToLocalFile(new Path("hdfs://192.168.37.101:9000/aa/qingshu2.txt"), new Path("c:/qingshu2.txt"));

    }

    /**
     * 查看文件信息
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     *
     */
    @Test
    public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

        // listFiles列出的是文件信息，而且提供递归遍历
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            Path filePath = file.getPath();
            String fileName = filePath.getName();
            System.out.println(fileName);

        }

        System.out.println("---------------------------------");

        //listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status: listStatus){

            String name = status.getPath().getName();
            System.out.println(name + (status.isDirectory()?" is dir":" is file"));

        }

    }


    /**
     * 删除文件或文件夹
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void rm() throws IllegalArgumentException, IOException {

        fs.delete(new Path("/aa"), true);

    }



    /**
     * ls 查询列表
     * @throws IOException
     */
    public void listFile(Path path) throws IOException {
        //
        FileStatus[] fslist = fs.listStatus(path);
        for ( FileStatus  fs: fslist) {

            if (fs.isDirectory()){
                System.out.println("文件夹："+fs.getPath().getName());
                listFile(fs.getPath());
             }
            System.out.println("文件："+fs.getPath().getName());

        }
    }

    /**
     * 第归查询ls
     * @throws IOException
     */
    @Test
    public void listFile() throws IOException {

        listFile(new Path("/"));
    }

    @Test
    public void catFile() throws IOException {

        //取巧的办法，不建议使用。
        //正常的使用方法是定义数组长度为1024的倍数，然后进循环，这样才是标准的。

        Path path=new Path("/aa/aa.txt");
        //根据path路径获取FileStatus对象
        FileStatus fileLinkStatus = fs.getFileLinkStatus(path);
        //文件的字节长度
        long len = fileLinkStatus.getLen();
        //字节数组
        byte[] b=new byte[(int)len];
        //获得需要叉开的文件的输入流
        FSDataInputStream fsDataInputStream = fs.open(path);
        //通过输入流读取数据
        fsDataInputStream.read(b);
        System.out.println(new String(b));

    }

    /**
     * cat
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        //获取输入流
        FSDataInputStream fsis = fs.open(new Path("hdfs://hadoop:9000/aaa/bbb/ccc/aa.txt"));
        //字节数组
        byte[] b=new byte[1024];
        //通过输入流读取内容
        int len = 0;
        while ((len=fsis.read(b))!=-1){
            //转换成String
            String s= new String(b,0,len);
            //打印输结果
            System.out.println(s);
        }


    }


}
