package com.qbk;

import lombok.extern.java.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.sqoop.Sqoop;
//import org.apache.sqoop.hive.HiveConfig;
//import org.apache.sqoop.tool.SqoopTool;
//import org.apache.sqoop.util.OptionsFileUtil;


/**
 * hive测试
 */
@Log
@RunWith(SpringRunner.class)
@SpringBootTest
public class HiveTest {


    @Autowired
    @Qualifier("hiveDruidTemplate")
    private JdbcTemplate hiveDruidTemplate;

    @Autowired
    @Qualifier("hiveDruidDataSource")
    DataSource jdbcDataSource;

    /**
     * 查询所有表
     */
    @Test
    public void listAllTables() throws SQLException {
        List<String> list = new ArrayList<String>();
        Statement statement = jdbcDataSource.getConnection().createStatement();
        String sql = "show tables";
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        System.out.println(list);
    }



//    @Test
//    public void sqoopList() throws Exception {
//        String[] args = new String[]{
//                "--connect","jdbc:mysql://192.168.11.233:3306/db03?useSSL=false&serverTimezone=GMT%2B8",
//                "--driver","com.mysql.jdbc.Driver",
//                "--username","root",
//                "--password","123456" ,
//                "--table","tab_user",
//                "--export-dir","/user/hive/warehouse/tab_user/part-m-00000",
//                "--input-fields-terminated-by","'\\0001'",
//                "--input-lines-terminated-by","'\\n'",
//        };
//        String[] expandArguments = OptionsFileUtil.expandArguments(args);
//        SqoopTool tool = SqoopTool.getTool("export");
//        Configuration conf = new Configuration();
//        //设置HDFS服务地址
//        conf.set("fs.default.name", "hdfs://192.168.11.241:9000");
//        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
//        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool ,loadPlugins);
//        int res = Sqoop.runSqoop(sqoop, expandArguments);
//        if (res == 0){
//            log.info ("成功");
//        }else {
//            log.info("失败");
//        }
//    }





}
