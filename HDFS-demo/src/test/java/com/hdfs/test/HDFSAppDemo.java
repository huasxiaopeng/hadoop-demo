package com.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName HDFSAppDemo
 * @Description TODO
 * @Author lktbz
 * @Date 2020/7/5
 */
public class HDFSAppDemo {
    private static final String HOST="hdfs://192.168.5.139:9000";
    Configuration configuration=null;
    FileSystem fileSystem=null;
    URI uri;
    Path path;
    @Before
    public void before() throws Exception {
        System.out.println("-----bdfore-------");
        uri=new URI(HOST);
        configuration=new Configuration();
        fileSystem=FileSystem.get(uri,configuration,"hadoop");

    }
//    @After
//    public void after(){
//        System.out.println("-----after-------");
//        configuration=null;
//        fileSystem=null;
//    }
    /**
     * 创建文件夹方式
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void  test01() throws URISyntaxException, IOException, InterruptedException {
        //远程服务 core.site 配置的默认地址
//        URI uri=new URI("hdfs://192.168.5.139:9000");
//        Configuration configuration=new Configuration();
//        FileSystem fileSystem=FileSystem.get(uri,configuration,"hadoop");
//        Path path=new Path("/lktbz/yan");
//        boolean bool = fileSystem.mkdirs(path);
//           if(bool){System.out.println("succesd");}

        path=new Path("/jfhksd");
        boolean mkdirs = fileSystem.mkdirs(path);

    }

    /**
     * 读取文件内容
     */
    @Test
    public void  test02() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/README.txt"));
        IOUtils.copyBytes(open,System.out,1024);
    }

    /**
     * 推送
     * @throws IOException
     */
    @Test
    public void  test03() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/lktbz/yan/a.txt"));
        fsDataOutputStream.writeUTF("lalla我在测试推送内容哟。。。。");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 拷贝本地数据到远程
     * @throws IOException
     */
    @Test
    public void  test04() throws IOException {
        Path local=new Path("/usr/local/hadoop/etc/hadoop/log4j.properties");
        Path remote=new Path("/");
        fileSystem.copyFromLocalFile(local,remote);
    }

    /**
     * 拷贝大文件带进度条
     * @throws IOException
     */
    @Test
    public void  test05() throws IOException {
        InputStream inputStream=new BufferedInputStream(new FileInputStream(new File("D:javatools")));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/lktbz/yan/jdk.tar"), new Progressable() {
            @Override
            public void progress() {
                System.out.println("-");
            }
        });
        IOUtils.copyBytes(inputStream,fsDataOutputStream,4096);
    }
}
