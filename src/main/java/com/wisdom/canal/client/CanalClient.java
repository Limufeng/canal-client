package com.wisdom.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wisdom.canal.config.CommonUtil;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @date: 2020/9/2 10:31
 * @author: LJP
 * @description: canal客户端类
 */
@Component
public class CanalClient {
    Logger logger = LoggerFactory.getLogger(CanalClient.class);
    /*sql队列*/
    private Queue<Map<String, String>> SQL_QUEUE = new ConcurrentLinkedQueue<>();

    @Resource
    @Qualifier("towerDataSource")
    private DataSource towerDataSource;

    @Resource
    @Qualifier("elevatorDataSource")
    private DataSource elevatorDataSource;

    @Resource
    @Qualifier("dustDataSource")
    private DataSource dustDataSource;

    @Resource
    @Qualifier("cloudDataSource")
    private DataSource cloudDataSource;

    /**
     * @return canal同步数据
     */
    public void run() {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1",
                11111), "example", "", "");//根据文档修改
        int batchSize = 1000;
        try {
            while (true) {
                logger.info("-------------开始连接-------------");
                try {
                    connector.connect();
                    connector.subscribe(".*\\..*");
                    connector.rollback();
                } catch (Exception e) {
                    logger.info("-------------连接失败,五分钟后尝试重新连接-------------");
                    try {
                        Thread.sleep(300000);
                    } catch (InterruptedException e1) {
                        logger.error(e1.getMessage());
                    }
                }
                logger.info("-------------连接成功-------------");
                break;
            }
            while (true) try {
                /*从master拉取数据batchSize条记录*/
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    Thread.sleep(10000);
                } else {
                    dataHandle(message.getEntries());
                }
                /*提交ack确认*/
                connector.ack(batchId);
                /*设置队列sql语句执行最大值*/
                if (SQL_QUEUE.size() >= 1) {
                    executeQueueSql();
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("canal入库方法" + e.getMessage());
            }

        } finally {
            connector.disconnect();
        }
    }

    /**
     * 执行队列中sql语句
     */
    public void executeQueueSql() {
        int size = SQL_QUEUE.size();
        for (int i = 0; i < size; i++) {
            Map<String, String> sql = SQL_QUEUE.poll();
            if (sql != null) {
                this.execute(sql);
            }
        }
    }

    /**
     * @param entryS
     * @return 数据处理
     */
    private void dataHandle(List<Entry> entryS) throws InvalidProtocolBufferException {
        for (Entry entry : entryS) {
            if (EntryType.ROWDATA == entry.getEntryType()) {
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                EventType eventType = rowChange.getEventType();
                if (eventType == EventType.DELETE) {
                    saveDeleteSql(entry);
                } else if (eventType == EventType.UPDATE) {
                    saveUpdateSql(entry);
                } else if (eventType == EventType.INSERT) {
                    saveInsertSql(entry);
                } else if (eventType == EventType.ERASE) {
                    SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), rowChange.getSql()));
                    /*logger.info("删除数据表[sql]--->" + rowChange.getSql());*/
                } else if (eventType == EventType.ALTER) {
                    SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), rowChange.getSql()));
                    /*logger.info("修改数据表[sql]--->" + rowChange.getSql());*/
                } else if (eventType == EventType.CREATE) {
                    SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), rowChange.getSql()));
                    /*logger.info("新建数据表[sql]--->" + rowChange.getSql());*/
                } else {
                    logger.info("其他类型数据操作不保存---->" + RowChange.parseFrom(entry.getStoreValue()));
                }
            }
        }
    }

    /**
     * @param entry
     * @return 保存更新语句
     */
    private void saveUpdateSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDataList = rowChange.getRowDatasList();
            for (RowData rowData : rowDataList) {
                List<Column> newColumnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("update " + entry.getHeader().getTableName() + " set ");
                for (int i = 0; i < newColumnList.size(); i++) {
                    sql.append(" " + newColumnList.get(i).getName()
                            + " = '" + newColumnList.get(i).getValue() + "'");
                    if (i != newColumnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(" where ");
                List<Column> oldColumnList = rowData.getBeforeColumnsList();
                for (Column column : oldColumnList) {
                    if (column.getIsKey()) {
                        /*暂时只支持单一主键*/
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                /*logger.info("保存更新语句===[sql]----> " + sql.toString());*/
                SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), sql.toString()));
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param entry
     * @return 保存删除语句
     */
    private void saveDeleteSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDataList = rowChange.getRowDatasList();
            for (RowData rowData : rowDataList) {
                List<Column> columnList = rowData.getBeforeColumnsList();
                StringBuffer sql = new StringBuffer("delete from " + entry.getHeader().getTableName() + " where ");
                for (Column column : columnList) {
                    if (column.getIsKey()) {
                        /*暂时只支持单一主键*/
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                /*logger.info("保存删除语句===[sql]----> " + sql.toString());*/
                SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), sql.toString()));
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param entry
     * @return 保存插入语句
     */
    private void saveInsertSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDataList = rowChange.getRowDatasList();
            for (RowData rowData : rowDataList) {
                List<Column> columnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("insert into " + entry.getHeader().getTableName() + " (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append(columnList.get(i).getName());
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(") VALUES (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append("'" + columnList.get(i).getValue() + "'");
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                /*logger.info("保存插入语句===[sql]----> " + sql.toString());*/
                SQL_QUEUE.add(getSql(entry.getHeader().getSchemaName(), sql.toString()));
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param schemaName 数据库名
     * @param sql        sql语句
     * @return 存储sql的map
     */
    public Map<String, String> getSql(String schemaName, String sql) {
        Map<String, String> map = new HashMap<>();
        map.put(schemaName, sql);
        return map;
    }

    /**
     * @param sql
     * @return 入库
     */
    public void execute(Map<String, String> sql) {
        Connection con = null;
        try {
            for (Map.Entry<String, String> entry : sql.entrySet()) {
                if (null == entry.getValue()) {
                    return;
                }
	//数据库自行修改
                if (entry.getKey().equals(CommonUtil.TOWER)) {
                    con = towerDataSource.getConnection();
                } else if (entry.getKey().equals(CommonUtil.ELEVATOR)) {
                    con = elevatorDataSource.getConnection();
                } else if (entry.getKey().equals(CommonUtil.DUST)) {
                    con = dustDataSource.getConnection();
                } else if (entry.getKey().equals(CommonUtil.CLOUD)) {
                    con = cloudDataSource.getConnection();
                } else {
                    logger.info("[-----不需存储-----]");
                }
                QueryRunner qr = new QueryRunner();
                if (con != null) {
                    qr.execute(con, entry.getValue().replace("''", "null"));
                    logger.info("[入库sql]--->" + entry.getValue());
                }
            }
        } catch (SQLException e) {
            logger.error("[sql入库异常]--->" + e.getNextException() + "<---[入库异常sql]");
        } finally {
            DbUtils.closeQuietly(con);
        }
    }
}