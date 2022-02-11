package com.kunghsu.apache.flink.tableapi.hive.task;

import com.kunghsu.apache.flink.tableapi.hive.dao.HiveInfoDao;
import com.kunghsu.apache.flink.tableapi.hive.entity.PeopleHiveDO;
import com.kunghsu.common.utils.JacksonUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分区查询demo
 * 默认查询最新分区
 * author:xuyaokun_kzx
 * date:2022/2/8
 * desc:
*/
public class HiveQueryPartitionDemoTask {

    private final static Logger LOGGER = LoggerFactory.getLogger(HiveQueryPartitionDemoTask.class);

    public static void main(String[] args) throws Exception {

        //开始执行
        System.out.println("HiveQueryPartitionDemoTask start");

        //hive相关属性
        //定义一个唯一的名称，这个值是可以随意定义的
        String catalogName = "myhive";
        //hive-site.xml的正确位置
        String hiveConfDir = "D:\\hive\\apache-hive-2.3.6-bin\\conf";
        String version = "2.3.6";
        String database = "test";

        try {
            //很关键，在这里就要调table api
            String sql = "SELECT id, name, destination FROM people_partition4 "
                    + "/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.partition.include' = 'latest') */" +
//                    ", 'streaming-source.monitor-interval' = '1 h',\n" +
//                    "    'streaming-source.partition-order' = 'partition-name'
                    "where name = 'name1'"
                    ;

            //带分区查询
            String sql2 = "SELECT id, name, destination FROM people_partition4 " +
                    "where name = 'name1' and partstart = '20220208'"
                    ;

            //初始化table对象
            HiveInfoDao.init(catalogName, hiveConfDir, version, database);
            Table table = HiveInfoDao.query(sql2);
            //执行拿到结果
            TableResult tableResult = table.execute();
            CloseableIterator<Row> rowCloseableIterator = tableResult.collect();
            //解析结果
            while (rowCloseableIterator.hasNext()){
                Row row = rowCloseableIterator.next();
                PeopleHiveDO peopleHiveDO = new PeopleHiveDO();
                //拿到第一列
                peopleHiveDO.setId((Integer) row.getField(0));
                peopleHiveDO.setName((String) row.getField(1));
                peopleHiveDO.setDestination((String) row.getField(2));
                System.out.println(JacksonUtils.toJSONString(peopleHiveDO));
            }
            System.out.println("查询结束");
        }catch (Throwable e){
            System.out.println("查询hive异常");
            e.printStackTrace();
        }

    }

}
