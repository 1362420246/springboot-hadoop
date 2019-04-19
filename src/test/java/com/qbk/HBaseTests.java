package com.qbk;

import com.qbk.hbase.HBaseUtil;
import lombok.extern.java.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;

/**
 * HBase 测试
 */
@Log
@RunWith(SpringRunner.class)
@SpringBootTest
public class HBaseTests {


//    @Autowired
//    private HBaseConfig con;

//    @Autowired
//    private Connection connection ;


    /**
     *  创建表
     */
	@Test
	public void createTable() throws IOException {
        int result = HBaseUtil.createTable("qbk4", new String[]{"a", "b","c"});
        if(result > 0){
            System.out.println("创建成功");
        }else if(result == 0){
            System.out.println("表已存在");
        }else {
            System.out.println("创建失败");
        }
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable() {
        int result = HBaseUtil.dropTable("qbk1");
        if(result > 0){
            System.out.println("删除成功");
        }else if(result == 0){
            System.out.println("表不存在");
        }else {
            System.out.println("删除失败");
        }
    }

    /**
     * 插入或者更新数据
     */
    @Test
    public void insertOrUpdate() {
        //列蔟必须存在   列可以不存在
        boolean result = HBaseUtil.insertOrUpdate("qbk4","1","a","alias","quboka");
            HBaseUtil.insertOrUpdate("qbk4","1","a","name","quboka");
            HBaseUtil.insertOrUpdate("qbk4","1","b","bname","quboka");
            HBaseUtil.insertOrUpdate("qbk4","1","c","cname","quboka");
        if(result){
            System.out.println("成功");
        }else {
            System.out.println("失败");
        }
    }

    /**
     *  删除 delRow  delFamily  delColumn
     */
    @Test
    public void delete() {
        boolean result = HBaseUtil.delColumn("qbk3","1","a","alias");
        if(result){
            System.out.println(" 删除一行下的一个列族下的一个列成功");
        }else {
            System.out.println(" 删除一行下的一个列族下的一个列失败");
        }
        boolean result2 = HBaseUtil.delFamily("qbk3","1","a");
        if(result2){
            System.out.println("删除一行下的一个列族成功");
        }else {
            System.out.println("删除一行下的一个列族失败");
        }
        boolean result3 = HBaseUtil.delRow("qbk3","1");
        if(result3){
            System.out.println("删除一行成功");
        }else {
            System.out.println("删除一行失败");
        }
    }

    /**
     * 获取单列下的值
     */
    @Test
    public void getValue() {
        String result = HBaseUtil.getValue("qbk4","1","a","alias");
        System.out.println("结果："+result);
    }

    /**
     *  获取单个列族下的键值对
     */
    @Test
    public void getFamilyValue() {
        Map<String,String> result = HBaseUtil.getFamilyValue("qbk4","1","a");
        System.out.println("结果："+result);
    }

    /**
     *  取到多个列族的键值对
     */
    @Test
    public void getFamilyListValue() {
        Map<String, Map<String, String>> result = HBaseUtil.getFamilyListValue("qbk4","1");
        System.out.println("结果："+result);
    }

}
