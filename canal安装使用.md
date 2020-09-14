# canal数据库同步

### 准备 0：binlog文件
#### 0.1：binlog文件包含两种类型：
- 索引文件（文件名后缀为.index）用于记录哪些日志文件正在被使用
- 日志文件（文件名后缀为.00000*）记录数据库所有的DDL和DML(除了数据查询语句)语句事件。

1. 索引文件大小：我们可以通过 max_binlog_size  参数设置binlog文件的大小。Binlog最大值，最大和默认值是1GB，该设置并不能严格控制Binlog的大小，尤其是Binlog比较靠近最大值而又遇到一个比较大事务时，为了保证事务的完整性，不可能做切换日志的动作，只能将该事务的所有SQL都记录进当前日志，直到事务结束
2. 索引文件删除：binlog的删除可以手工删除或自动删除。通过设置 expire_logs_days 实现自动删除 
3. 手动删除需登录mysql后执行如下命令：

```
mysql> reset master;        //删除master的binlog，即手动删除所有的binlog日志
mysql> reset slave;          //删除slave的中继日志
mysql> purge master logs before '2019-07-07 17:20:00';         //删除指定日期以前的日志索引中binlog日志文件
mysql> purge master logs to 'binlog.000003';       //删除指定日志文件的日志索引中binlog日志文件
```
#### 0.2：binlog一共有三种格式：
| 格式        | 定义                 | 优点              | 缺点                                                                                                                                                                                           |
|-----------|--------------------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| statement | 记录的是修改SQL语句        | 日志文件小，节约IO，提高性能 | 准确性差，对一些系统函数不能准确复制或不能复制，如now()、uuid()、limit(由于mysql是自选索引，有可能master同salve选择的索引不同，导致更新的内容也不同)等  在某些情况下会导致master-slave中的数据不一致(如sleep()函数， last_insert_id()，以及user-defined functions(udf)等会出现问题) |
| row       | 记录的是每行实际数据的变更      | 准确性强，能准确复制数据的变更 | 日志文件大，较大的网络IO和磁盘IO                                                                                                                                                                           |
| mixed     | statement和row模式的混合 | 准确性强，文件大小适中     | 当binlog format 设置为mixed时，普通复制不会有问题，但是级联复制在特殊情况下会binlog丢失。                                                                                                                                    |
mysql8.0中默认使用row模式，默认开启，不需要手动设置。

``` 
一些查看binlog命令（mysql8.0不用设置也可以）
登录mysql 查看binlog是否开启：show variables like 'log_bin';
修改binlog格式：SET GLOBAL BINLOG_FORMAT = 'STATEMENT';
查看写入的binlog文件：show master status;
查看记录的内容：show binlog events in 'binlogs.000003';/*日志文件名*/
```

### 开始1：canal介绍
- 名称：canal [kə'næl]
- 译意： 水道/管道/沟渠
- 语言： 纯java开发
- 定位： 基于数据库增量日志解析，提供增量数据订阅&消费，目前主要支持了mysql
- 关键词： mysql binlog parser / real-time / queue&topic
#### 1.1：mysql主备工作原理
1. master将改变记录到二进制日志(binary log)中（这些记录叫做二进制日志事件，binary log events，可以通过show binlog events进行查看）；
2. slave将master的binary log events拷贝到它的中继日志(relay log)；
3. slave重做中继日志中的事件，将改变反映它自己的数据。
![canal工作原理](https://images.gitee.com/uploads/images/2020/0914/112355_4b983d94_5406216.png)
#### 1.2：canal工作原理
1. canal模拟mysql slave的交互协议，伪装自己为mysql slave，向mysql master发送dump协议
2. mysql master收到dump请求，开始推送binary log给slave(也就是canal)
3. canal解析binary log对象(原始为byte流)
![canal工作原理](https://images.gitee.com/uploads/images/2020/0914/112716_9bcc35ba_5406216.png)

#### 1.3：安装canal前准备
- 对于自建 MySQL , 需要先开启 Binlog 写入功能，配置 binlog-format 为 ROW 模式，my.cnf 中配置如下
```
[mysqld]
log-bin=mysql-bin # 开启 binlog
binlog-format=ROW # 选择 ROW 模式
server_id=1 # 配置 MySQL replaction 需要定义，不要和 canal 的 slaveId 重复
```
ps:msyql8.0默认已开启，可跳过。
- 授权 canal 链接 MySQL 账号具有作为 MySQL slave 的权限, 如果已有账户可直接 grant

```
开启一个有远程权限账号（ps:可用root）
CREATE USER canal IDENTIFIED BY 'canal';  
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;
```
#### 1.4canal安装教程

1.  下载 canal, 访问 release 页面 , 选择需要的包下载, 如以 1.1.5 版本为例
```
https://github.com/alibaba/canal/releases/download/canal-1.1.5-alpha-2/canal.deployer-1.1.5-SNAPSHOT.tar.gz
```
2.  解压缩

```
mkdir /tmp/canal    # 可自己指定目录
tar zxvf canal.deployer-$version.tar.gz  -C /tmp/canal
```
![目录结构](https://images.gitee.com/uploads/images/2020/0914/115655_490e1ad1_5406216.png)

3.  配置修改（ps：根据需要修改）

```
vim conf/example/instance.properties
```
![配置信息](https://images.gitee.com/uploads/images/2020/0914/120138_bc3e5a70_5406216.png)

```
#ps：此文件可默认，连接时打开端口11111即可。
vim conf/canal.properties 
sh bin/startup.sh
# 启动
sh bin/stop.sh
# 关闭
# 查看 logs日志 canal无报错即可
# 查看 server 日志
vim logs/canal/canal.log
# 查看 instance 的日志
vim logs/example/example.log
#（ps：注意内存太少可能启动失败，我得至少0.5G内存才启动成功）
```
![配置修改](https://images.gitee.com/uploads/images/2020/0914/133724_f35aa172_5406216.png)

#### 2：注意事项

- 服务器端口号注意开启11111端口号（ps：默认，可修改）
ps：canal get数据超时时间修改
![canal get数据超时时间修改](https://images.gitee.com/uploads/images/2020/0914/165312_87b5c940_5406216.png)

#### 3：java代码

```
<!-- 依赖 -->
<dependency>
    <groupId>commons-dbutils</groupId>
    <artifactId>commons-dbutils</artifactId>
    <version>1.7</version>
</dependency>
<dependency>
    <groupId>com.alibaba.otter</groupId>
    <artifactId>canal.client</artifactId>
    <version>1.1.4</version>
</dependency>
```

```
<!-- 测试代码 -->
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
/*master服务器地址，以及开启的端口号，实例名instance.properties文件中不修改这用这个默认的，用户名密码使用远程连接账号的，同前配置文件*/
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1",
                11111), "example", "", "");
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
                /*多数据源配置，匹配不同数据库equals（数据库名）*/
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
```

```
/*启动master数据库监听*/
@SpringBootApplication
public class CanalApplication implements CommandLineRunner {
    @Resource
    private CanalClient canalClient;

    public static void main(String[] args) {
        SpringApplication.run(CanalApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        /*启动canal客户端监听*/
        canalClient.run();
    }
}
```


