package com.qbk;

import lombok.extern.java.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS 测试
 * https://www.zifangsky.cn/1294.html
 */
@Log
@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsTest {
    /**
     *  etc/hadoop/core-site.xml 文件中配置的地址和端口
     */
    private static final String HDFS_PATH = "hdfs://hadoop1:9000" ;
    /**
     * Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。
     */
    FileSystem fileSystem = null ;
    Configuration configuration = null ;
    /**
     * 初始化
     */
    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        //获取HDFS文件系统
        fileSystem = FileSystem.get(URI.create(HDFS_PATH),configuration,"root");
    }
    /**
     * 创建目录
     */
    @Test
    public void mdkir() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/qbk/test2"));
    }
    /**
     * 创建文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream output = fileSystem.create(new Path("/qbk/test/hello.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }
    /**
     * 查看hdfs文件的内容
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream input = fileSystem.open(new Path("/qbk/test/a.txt"));
        //copy
        IOUtils.copyBytes(input,System.out,1024);
        input.close();
    }
    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/qbk/test/a.txt");
        Path newPath = new Path("/qbk/test/b.txt");
        boolean rename = fileSystem.rename(oldPath, newPath);
    }
    /**
     * 上传文件到hdfs
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        //本地路径
        Path localPath = new Path("C:/Users/86186/Desktop/hello.txt");
        //hdfs路径
        Path hdfsPath = new Path("/qbk/test");
         fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }
    /**
     * 上传文件到hdfs(升级版)
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:/java开发工具/hadoop/原生/hadoop-3.1.1.tar.gz")));
        FSDataOutputStream output = fileSystem.create(
                new Path("/qbk/test/hadoop-3.1.1.tar.gz"), new Progressable() {
                    public void progress() {
                        //进度提醒消息
                        System.out.println("上传中...");
                }
                });
        IOUtils.copyBytes(in,output,4096);
    }
    /**
     * 下载hdfs文件
     * 存在windows问题：https://wiki.apache.org/hadoop/WindowsProblems
     * 答案：不需要安装hadoop，但是需要配置HADOOP_HOME变量。
     * 解决方案：根据自己服务器版本，下载相应的winutils
     *  1：将文件解压
     *  2：将hadoop.dll复制到C:\Window\System32下
     *  3：添加环境变量HADOOP_HOME，指向hadoop目录
     *  4：将%HADOOP_HOME%\bin加入到path里面
     *  5：重启 IDE（你的编辑工具，例如eclipse，intellij idea）
     */
    @Test
    public void copyToLocalFile() throws IOException {
        //本地路径
        Path localPath = new Path("C:/Users/86186/Desktop/sss.txt");
        //hdfs路径
        Path hdfsPath = new Path("/hello.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }
    /**
     * 查询一个目录下的所有文件
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses =fileSystem.listStatus(new Path("/qbk/test"));
        for (FileStatus fileStatue: fileStatuses ) {
            String isDir =fileStatue.isDirectory() ? "文件夹" : "文件" ;
            //副本系数
            short replication = fileStatue.getReplication();
            //大小
            long len = fileStatue.getLen();
            //路径
            String path = fileStatue.getPath().toString();
            System.out.println(isDir + "\t" +replication + "\t" + len + "\t" + path );
            /* 结果：
                文件	3	12	hdfs://192.168.11.234:9000/qbk/test/b.txt
                文件	3	334559382	hdfs://192.168.11.234:9000/qbk/test/hadoop-3.1.1.tar.gz
                文件	3	332433589	hdfs://192.168.11.234:9000/qbk/test/hadoop-3.1.2.tar.gz
                文件	3	17	hdfs://192.168.11.234:9000/qbk/test/hello.txt
               问题：在hdfs-site.xml中设置副本系数为1，为什么此时查到是3？
                如果通过hdfs shell 方式put上去的文件，才采用默认的副本系数1
                如果通过java api 上传上去的，在本地我们并没有手工设置副本系数，所以采用的是haoop自己的副本系数
             */
        }
    }
    /**
     * 删除
     */
    @Test
    public void delete() throws IOException {
        //是否递归删除
        boolean delete = fileSystem.delete(new Path("/output"), true);
        System.out.println(delete);
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("HdfsTest.tearDown");
    }
}
