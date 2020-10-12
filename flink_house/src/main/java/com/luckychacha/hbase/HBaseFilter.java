package com.luckychacha.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * @author leixinxin
 * @date 2020-10-06 10:37
 */
public class HBaseFilter {
    public static void main(String[] args) throws  Exception {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table myuser = connection.getTable(TableName.valueOf("flink:data_orders"));
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter("f1".getBytes(), "createTime".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, "2019-09-06 06:37:34".getBytes());
        SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter("f1".getBytes(), "createTime".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, "2019-09-06 06:38:00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter1);
        filterList.addFilter(singleColumnValueFilter2);
        scan.setFilter(filterList);
        ResultScanner resultScanner = myuser.getScanner(scan);
        for (Result result : resultScanner) {
            //获取rowkey
            System.out.println(Bytes.toString(result.getRow()));
            //指定列族以及列打印列当中的数据出来
            System.out.println(Bytes.toString(result.getValue("f1".getBytes(), "realTotalMoney".getBytes())));
            System.out.println(Bytes.toString(result.getValue("f1".getBytes(), "createTime".getBytes())));
        }
        myuser.close();
    }
}
