package com.acco.gmall.realtime.common.util;

import com.acco.gmall.realtime.common.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import io.debezium.embedded.Connect;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * ClassName: HBaseUtil
 * Description: 获取连接
 * Package: com.acco.gmall.realtime.common.util
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-25 14:27
 */
public class HBaseUtil {
    /**
     * 获取连接
     * @return 返回一个hase连接(同步)
     */
    public static Connection getConnection()  {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM); //habase里面的配置

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return connection;
        // 可以使配置文件,放在resouce里面,文件地址  /opt/module/hbase-2.4.14/conf/hbase-site.xml
    }

    /**
     * 关闭连接
     * @param connection
     */
    public  static void closeConnection(Connection connection){
        if (connection == null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     *
     * @param connection 连接
     * @param nameSpace 空间名
     * @param table 表明
     * @param family 建表字段
     * @throws IOException
     */
    public static void createTable(Connection connection, String nameSpace, String table,String... family) throws IOException {
        if (family.length == 0 || family == null){
            System.out.println("Hbase 至少需要一个列族");
            return ;
        }
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder
                = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));
        for (String s : family) {
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(s))
                    .build();
            tableDescriptorBuilder.setColumnFamily((build));
        }
        // 使用admin创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println(nameSpace+"."+table+"已存在，无需创建.");;
            admin.close();
        }
        admin.close(); // 关闭连接
    }

    /**
     * 删除表
     * @param connection 连接
     * @param nameSpace 空间
     * @param table 表明
     * @throws IOException
     */
    public static void dropTable(Connection connection, String nameSpace, String table) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(nameSpace,table));
            admin.deleteTable(TableName.valueOf(nameSpace,table));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        admin.close();
    }

    /**
     * 写数据到Hbase
     * @param connection 连接
     * @param nameSpace 空间名
     * @param tableName 表明
     * @param primarilyKey 主键
     * @param family 列族名
     * @param data 列明和列支 Json对象
     * @throws IOException
     */
    public static void  putCells(Connection connection, String nameSpace, String tableName, String primarilyKey,String family, JSONObject data) throws IOException {
        // 获取tale
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        // 创建写入对象
        Put put = new Put(Bytes.toBytes(primarilyKey));
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
            }
        }
        // 调用方法写入数据
        try {
            table.put(put);
        } catch (IOException e) {
            System.out.println("写入错误："+put.toString());
        }
        table.close();


    }

    /**
     * 删除一整行数据
     * @param connection 连接
     * @param nameSpace 空间名
     * @param tableName 表明
     * @param primarilyKey 主键
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String nameSpace, String tableName, String primarilyKey) throws IOException {
        // 获取表格
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        //创建删除对象
        Delete delete = new Delete(Bytes.toBytes(primarilyKey));
        // 删除
        try {
            table.delete(delete);
        } catch (IOException e) {
            System.out.println("删除错误"+delete.toString());
        }
        // 关闭
        table.close();

    }
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String table,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table Table = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) { // jdk1.7 : 可以自动释放资源
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = Table.get(get);
            // 4. 把查询到的一行数据,封装到一个对象中: JSONObject
            // 4.1 一行中所有的列全部解析出来
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            T t = tClass.newInstance();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                }
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                BeanUtils.setProperty(t, key, value);
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static JSONObject getCells(Connection connection,String namespace,String tableName, String rowKey) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 调用get方案
        Result result = table.get(get);
        JSONObject jsonObject = new JSONObject();
        for (Cell cell : result.rawCells()) {
            // 很底层 需要转化
            jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),new String(CellUtil.cloneValue(cell),StandardCharsets.UTF_8));
        }

        // 关闭连接
        connection.close();

        return jsonObject;

    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }


}
