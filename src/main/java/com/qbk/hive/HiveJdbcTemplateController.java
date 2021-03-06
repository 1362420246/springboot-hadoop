package com.qbk.hive;

import com.qbk.util.ProcessUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.apache.hadoop.conf.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * 使用 JdbcTemplate 操作 Hive
 *
 * https://blog.csdn.net/pengjunlee/article/details/81838480#commentBox
 */
@Slf4j
@RestController
@RequestMapping("/hive")
public class HiveJdbcTemplateController {

    @Autowired
    @Qualifier("hiveDruidTemplate")
    private JdbcTemplate hiveDruidTemplate;

    @Autowired
    @Qualifier("hiveDruidDataSource")
    DataSource jdbcDataSource;


    /**
     * TODO
     * https://www.cnblogs.com/claren/p/7240735.html
     * 执行 sqoop api  存在maven导入失败问题
     * 可以自行打包 也可以指定jar包实际路径
     */
    @RequestMapping("/test")
    public int sqoop() throws Exception {

        String curClasspath = System.getProperty ("java.class.path");
        curClasspath = curClasspath
                + File.pathSeparator
                + "/opt/sqoop-1.4.7/sqoop-1.4.7.jar"
                + File.pathSeparator
                + "/opt/jar/hadoop-common-2.6.0-cdh5.4.4.jar";
        System.setProperty ("java.class.path", curClasspath);

        String[] args = new String[]{
                "--connect","jdbc:mysql://192.168.11.233:3306/db03?useSSL=false&serverTimezone=GMT%2B8",
                "--driver","com.mysql.jdbc.Driver",
                "--username","root",
                "--password","123456" ,
                "--table","tab_user",
                "--export-dir","/user/hive/warehouse/tab_user/part-m-00000",
                "--input-fields-terminated-by","'\\0001'",
                "--input-lines-terminated-by","'\\n'",
                "--hadoop-mapred-home","/opt/hadoop-3.1.2"
        };
        String[] expandArguments = OptionsFileUtil.expandArguments(args);
        SqoopTool tool = SqoopTool.getTool("export");
        Configuration conf = new Configuration();
        //设置HDFS服务地址
        conf.set("fs.default.name", "hdfs://192.168.11.241:9000");
        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool ,loadPlugins);
        int res = Sqoop.runSqoop(sqoop, expandArguments);
        return res ;
    }

    /**
     * 执行 sqoop脚本
     */
    @RequestMapping("/sqoop")
    public int sqoopTest() throws InterruptedException, TimeoutException, IOException {
        return ProcessUtil.executeCommand("sh /opt/jar/sqoop.sh",10000000000L);
    }

    /**
     * 列举当前Hive库中的所有数据表
     */
    @RequestMapping("/table/list")
    public List<String> listAllTables() throws SQLException {
        List<String> list = new ArrayList<String>();
        Statement statement = jdbcDataSource.getConnection().createStatement();
        String sql = "show tables";
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }

    /**
     * 示例：创建新表
     */
    @RequestMapping("/table/create")
    public String createTable() {
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sql.append("user_sample");
        sql.append("(user_num BIGINT, user_name STRING, user_gender STRING, user_age INT)");
        // 定义分隔符
        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ");
        // 作为文本存储
        sql.append("STORED AS TEXTFILE");

        log.info("Running: " + sql);
        String result = "Create table successfully...";
        try {
            hiveDruidTemplate.execute(sql.toString());
        } catch (DataAccessException dae) {
            result = "Create table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;

    }

    /**
     * 示例：将Hive服务器本地文档中的数据加载到Hive表中
     */
    @RequestMapping("/table/load")
    public String loadIntoTable() {
        /** 文本中数据格式
         * 1,qubka,男,29
         * 2,zhangkai,男,26
         */
        String filepath = "/opt/jar/user_sample.txt";
        String sql = "load data local inpath '" + filepath + "' into table user_sample";
        String result = "Load data into table successfully...";
        try {
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Load data into table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }

    /**
     * 查询指定tableName表中的数据
     */
    @RequestMapping("/table/select")
    public List<String> selectFromTable(String tableName) throws SQLException {
        Statement statement = jdbcDataSource.getConnection().createStatement();
        String sql = "select * from " + tableName;
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        List<String> list = new ArrayList<String>();
        int count = res.getMetaData().getColumnCount();
        String str = null;
        while (res.next()) {
            str = "";
            for (int i = 1; i < count; i++) {
                str += res.getString(i) + " ";
            }
            str += res.getString(count);
            log.info(str);
            list.add(str);
        }
        return list;
    }

    /**
     * 示例：向Hive表中添加数据
     */
    @RequestMapping("/table/insert")
    public String insertIntoTable() {
        String sql = "INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES(888,'Plum','M',32)";
        String result = "Insert into table successfully...";
        try {
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Insert into table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }

    /**
     * 查询Hive库中的某张数据表字段信息
     */
    @RequestMapping("/table/describe")
    public List<String> describeTable(String tableName) throws SQLException {
        List<String> list = new ArrayList<String>();
        Statement statement = jdbcDataSource.getConnection().createStatement();
        String sql = "describe " + tableName;
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }

    /**
     * 示例：删除表
     */
    @RequestMapping("/table/delete")
    public String delete(String tableName) {
        String sql = "DROP TABLE IF EXISTS "+tableName;
        String result = "Drop table successfully...";
        log.info("Running: " + sql);
        try {
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Drop table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }

}