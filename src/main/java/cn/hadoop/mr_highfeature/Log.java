package cn.hadoop.mr_highfeature;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by yqy on 17-8-3.
 */
public class Log {
        //Logger实例
        public Logger logger = null;

        //将Log类封装为单例模式
        private static Log log;

        //构造函数，用于初始化Logger配置需要的属性
        private Log() {
            //获得当前目录路径
            //String filePath=this.getClass().getResource("/").getPath();
            //找到log4j.properties配置文件所在的目录(已经创建好)
            //filePath=filePath.substring(1).replace("bin", "src");
            //获得日志类logger的实例
            logger=Logger.getLogger(this.getClass());
            //logger所需的配置文件路径
            PropertyConfigurator.configure("cn/hadoop/mr_highfrature/log4j.properties");
        }

        public static Log getLogger() {
            if(log != null)
                return log;
            else
                return new Log();
        }

}
