package com.qbk.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * HBase工具
 */
@Component
public class HBaseUtil {

    @Autowired
    private Connection connection;

    private static Connection con;

    @PostConstruct
    private void init() throws Exception {
        if(this.connection == null ){
            throw new Exception() ;
        }
        con = connection ;
    }

    @PreDestroy
    public void dostory(){
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param FamilyColumn 列簇
     */
    public static int createTable(String tableName, String... FamilyColumn) {
        Admin admin = null;
        try {
            admin = con.getAdmin();
            /* 2x中过期方法
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            for (String fc : FamilyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
            }
            admin.createTable(htd);
             */

            //判断表是否可用
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                //添加列簇
                for (String familyName : FamilyColumn) {
                    tableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName)).build());
                }
                //创建表
                admin.createTable(tableDescriptor.build());
                return 1;
            }else {
                return 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除表
     */
    public static int dropTable(String tableName) {
        Admin admin = null;
        try {
            TableName tn = TableName.valueOf(tableName);
            admin = con.getAdmin();
            //判断表是否可用
            if(admin.isTableAvailable(tn)){
                admin = con.getAdmin();
                admin.disableTable(tn);
                admin.deleteTable(tn);
                return 1;
            }else {
                return 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 插入或者更新数据
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列簇
     * @param qualifier 列
     * @param value     值
     */
    public static boolean insertOrUpdate(String tableName, String rowKey,
                                 String family, String qualifier, String value) {
        Table table = null ;
        try {
            table = con.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 删除一行
     */
    public static boolean delRow(String tableName, String rowKey) {
        return delete(tableName, rowKey, null, null);
    }
    /**
     *  删除一行下的一个列族
     */
    public static boolean delFamily(String tableName, String rowKey, String family) {
        return delete(tableName, rowKey, family, null);
    }

    /**
     * 删除一行下的一个列族下的一个列
     */
    public static boolean delColumn(String tableName, String rowKey, String family, String qualifier) {
        return delete(tableName, rowKey, family, qualifier);
    }

    /**
     *  通用删除方法
     * @param tableName 表名
     * @param rowKey    行
     * @param family    列族
     * @param qualifier 列
     */
    public static boolean delete(String tableName, String rowKey, String family,
                              String qualifier) {
        Table table = null ;
        try {
            table = con.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            table.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *  获取单列下的值
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param qualifier 列
     */
    public static String getValue(String tableName, String rowKey, String family,
                               String qualifier) {
        Table table = null ;
        String value = "" ;
        try {
            table = con.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return value;
    }

    /**
     *  获取单个列族下的键值对
     * @param tableName 表名
     * @param rowKey    行
     * @param family    列族
     */
    public static Map<String, String> getFamilyValue(String tableName, String rowKey, String family) {
        Map<String, String> resultMap = new HashMap<>(16);
        Table table = null ;
        try {
            table = con.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result result = table.get(get);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    resultMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultMap;
    }


    /**
     *  取到多个列族的键值对
     * @param tableName 表名
     * @param rowKey    主键
     */
    public static Map<String, Map<String, String>> getFamilyListValue(String tableName, String rowKey) {
        Map<String, Map<String, String>> results = new HashMap<>(16);
        Table table = null ;
        try {
            table = con.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                    if (results.get(familyName) == null){
                        results.put(familyName, new HashMap<String,String> (16));
                    }
                    results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return results;
    }


    /**
     * 区间取值 TODO  Scan
     * @param tableName
     * @param family
     * @param column
     * @param startRow
     * @param stopRow
     *
     * https://www.cnblogs.com/frankdeng/p/9310209.html
     * https://blog.csdn.net/u014795084/article/details/80929644
     * https://blog.csdn.net/u010775025/article/details/80773679
     *
     */
    public List<String> getValueByStartStopRowKey(String tableName, String family, String column, String startRow, String stopRow) {
        Table table = null;
        Connection connection = null;
        List<String> rs = new ArrayList<>();
        try {

            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner result = table.getScanner(scan);
            result.forEach(r -> {
                Map map = r.getFamilyMap(Bytes.toBytes(family));
                List<Cell> cells = r.listCells();
                cells.forEach(c -> rs.add(Bytes.toString(CellUtil.cloneRow(c)) + ":::" + Bytes.toString(CellUtil.cloneValue(c))));
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }

}