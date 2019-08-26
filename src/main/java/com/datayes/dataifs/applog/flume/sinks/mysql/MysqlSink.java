package com.datayes.dataifs.applog.flume.sinks.mysql;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.datayes.dataifs.applog.flume.utils.Constant;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class MysqlSink extends AbstractSink implements Configurable{
    private String hostname;
    private String port;
    private String databaseName;
    private String user;
    private String password;
    private String tableName;
    private int expirationTime;
    private Connection conn;
    public static Object lock = new Object();

    private final CounterGroup counterGroup = new CounterGroup();
    private SinkCounter sinkCounter;

    private int batchSize;
    private static final String CONNURL = "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true";
    private static final String SELECTSQL = "select * from %s limit 0";
    private static final String INSERTSQL = "insert into %s (%s) values (%s)";
    private static final String DRIVER = "com.mysql.jdbc.Driver";

    public MysqlSink() {
        log.info("MysqlSink start...");
    }


    @Override
    public void configure(Context context) {
        hostname = context.getString(Constant.HOSTNAME);
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        tableName = context.getString(Constant.TABLENAME);
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        port = context.getString(Constant.PORT);
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString(Constant.DATABASENAME);
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        user = context.getString(Constant.USER);
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString(Constant.PASSWORD);
        Preconditions.checkNotNull(password, "password must be set!!");
        batchSize = context.getInteger(Constant.BATCHSIZE, 100);
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive number!!");
        expirationTime = context.getInteger(Constant.EXPIRATIONTIME, 120);
        TimerConnMap.setExpirationTime(expirationTime);
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

    }

    @Override
    public void start() {
        log.info("start mysqlSink");
        super.start();
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            log.error("ClassNotFountException", e);
        }
        refreshDbConn();
        sinkCounter.start();
        Timer tt = new Timer();//定时类
        tt.schedule(new TimerTask() {//创建一个定时任务
            @Override
            public void run() {
                synchronized (lock) {
                    refreshDbConn();
                }
            }
        }, 0, expirationTime * 1000);
    }

    private void refreshDbConn() {
        log.debug("start to refresh db connection");
        try {
            TimerConnMap.clear(conn);
            String url = String.format(CONNURL, hostname, port, databaseName);
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            log.error("fail to create db connection ", e);
        }
    }

    @Override
    public void stop() {
        log.info("stop method coming ");
        if (TimerConnMap.size() > 0) {
            for (PreparedStatement statement : TimerConnMap.getAllStatment()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    log.error("SQLException ", e);
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("SQLException ", e);
            }
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        String curTableName = null;
        try {
            int count;
            transaction.begin();
            List<Map<String, String>> resultList = new ArrayList<>();
            for (count = 0; count < batchSize; count++) {
                event = channel.take();
                if (event == null) {//对事件进行处理
                    break;
                }

                content = new String(event.getBody(), "utf-8");
                JSONObject jsonObject = JSONObject.parseObject(content);
                JSONObject commonJson = jsonObject.getJSONObject("common");
                JSONObject eventJson = jsonObject.getJSONObject("event");

                Map<String, String> resultMap = new HashMap<>();
                String appId = commonJson.getString("appId");
                if (StringUtils.isNotBlank(appId)) {
                    curTableName = String.format(tableName, appId);
//                        curTableName += ((StringUtils.isNotBlank(appEnv) && !appEnv.toUpperCase().equals("PRD")) ? ("_" + appEnv.toLowerCase()) : "");
                    resultMap.put("tableName", curTableName);
                } else {
                    continue;
                }

                Map<String, String> commonMap = JSONObject.parseObject(commonJson.toJSONString(), new TypeReference<Map<String, String>>() {
                });
                Map<String, String> eventMap = JSONObject.parseObject(eventJson.toJSONString(), new TypeReference<Map<String, String>>() {
                });

                resultMap.putAll(commonMap);
                resultMap.putAll(eventMap);
                if(resultMap.containsKey("url") && resultMap.get("url").length() > 250){
                    resultMap.put("url", resultMap.get("url").substring(0,250));
                }

                resultMap.put("detail", eventJson.toJSONString());
                resultMap.put("common", commonJson.toJSONString());

                resultList.add(resultMap);
            }
            int successCount = 0;
            if(count <= 0){
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if(count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(count);

                if (CollectionUtils.isNotEmpty(resultList)) {
                    log.info("start to handle {} records", resultList.size());
                    synchronized (lock) {
                        Map<String, PreparedStatement> tableStatment = new HashMap();
                        List<String> columnNames;
                        // 先批量插入， 若出錯，则回滚单条插
                        try {
                            for (Map<String, String> temp : resultList) {
                                String curtable = temp.get("tableName");
                                tableStatment.putIfAbsent(curtable, getPrepareStatement(curtable));
                                columnNames = getColumnNames(curtable);
                                for (int i = 0; i < columnNames.size(); i++) {
                                    tableStatment.get(curtable).setString(i + 1, temp.get(columnNames.get(i)));
                                }
                                tableStatment.get(curtable).addBatch();
                            }
                            for (PreparedStatement statment : tableStatment.values()) {
                                statment.executeBatch();
                            }
                            conn.commit();
                            successCount = resultList.size();
                            log.info("succeed in insert {} records of data", successCount);
                        } catch (Exception e) {
                            conn.rollback();
                            log.error("fail to batch insert data, rollback and try to insert one by one");
                            Integer failCount = 0;
                            for (Map<String, String> temp : resultList) {
                                String curtable = temp.get("tableName");
                                tableStatment.putIfAbsent(curtable, getPrepareStatement(curtable));
                                columnNames = getColumnNames(curtable);
                                for (int i = 0; i < columnNames.size(); i++) {
                                    tableStatment.get(curtable).setString(i + 1, temp.get(columnNames.get(i)));
                                }
                                try {
                                    tableStatment.get(curtable).execute();
                                    successCount++;
                                } catch (Exception e1) {
                                    failCount++;
                                    log.error("fail to insert data : " + JSONObject.toJSONString(temp), e1);
                                }
                            }
                            conn.commit();
                            log.info(String.format("finish handle %s records, succeed: %s, fail: %s", resultList.size(), successCount, failCount));
                        }
                    }
                }
            }
            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(successCount);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Exception e) {
            try {
                transaction.commit();
                sinkCounter.addToEventDrainSuccessCount(0);
                counterGroup.incrementAndGet("transaction.success");
            } catch (Exception ex) {
                log.error("Failed to commit transaction.", e);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    private List<String> getColumnNames(String tableName) {
        return TimerConnMap.getColumns(tableName);
    }

    private PreparedStatement getPrepareStatement(String tableName) {
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象
        if (TimerConnMap.containsKey(tableName)) {
            return TimerConnMap.getStatment(tableName);
        } else {
            log.info("no prepareStatment of table {}, start to create", tableName);
            String tableSql = String.format(SELECTSQL, tableName);
            try {
                //获取字段名
                PreparedStatement preparedStatement = conn.prepareStatement(tableSql);
                ResultSetMetaData rsmd = preparedStatement.getMetaData();
                int size = rsmd.getColumnCount();
                List<String> columnNames = new ArrayList<>();
                StringBuilder placeholders = new StringBuilder("?");//占位符
                for (int i = 1; i < size; i++) {
                    columnNames.add(rsmd.getColumnName(i + 1));
                }
                for (int i = 2; i < size; i++) {
                    placeholders.append(",?");
                }
                String columns = Joiner.on(",").join(columnNames);
                String insertSql = String.format(INSERTSQL, tableName, columns, placeholders);
                //创建一个Statement对象
                preparedStatement = conn.prepareStatement(insertSql);
                preparedStatement.clearBatch();
                log.info("finish create prepareStatment of table {}, insertSql: {}", tableName, insertSql);
                TimerConnMap.put(tableName, preparedStatement, columnNames);
                return preparedStatement;
            } catch (SQLException e) {
                log.error("SQLException ", e);
            }
        }
        return null;
    }

}

class TimerConnMap {
    private static Map<String, PreparedStatement> statmentMap = new ConcurrentHashMap<String, PreparedStatement>();//time主属性 用于存放 需要保存的字段
    private static Map<String, List<String>> columnMap = new ConcurrentHashMap<String, List<String>>();//time主属性 用于存放 需要保存的字段
    private static Map<String, Long> keytime = new HashMap<String, Long>();//time主属性 用于存放 需要保存的字段
    //	private static final long EXPIRATIONTIME=1000*60*90;//1个半小时
    private static long EXPIRATIONTIME = 1000 * 20;//测试用20秒
    private static final int START = 0;//设置执行开始时间
    private static final int INTERVAL = 10000;//设置间隔执行时间 单位/毫秒

    public static void setExpirationTime(int seconds) {
        EXPIRATIONTIME = 1000 * seconds;
    }

    public static void put(String key, PreparedStatement value, List<String> columnName) {
        statmentMap.put(key, value);
        columnMap.put(key, columnName);
        keytime.put(key, System.currentTimeMillis());
    }

    public static void clear(Connection conn) throws SQLException {
        synchronized (MysqlSink.lock) {
            if (conn != null) {
                for (PreparedStatement statement : statmentMap.values()) {
                    statement.executeBatch();
                }
                conn.commit();
            }
            statmentMap.clear();
            keytime.clear();
        }
    }

    public static boolean containsKey(String key) {
        return statmentMap.containsKey(key);
    }

    public static int size() {
        return statmentMap.size();
    }

    public static Collection<PreparedStatement> getAllStatment() {
        return statmentMap.values();
    }

    public static PreparedStatement getStatment(String key) {
        return statmentMap.get(key);
    }

    public static List<String> getColumns(String key) {
        return columnMap.get(key);
    }

    static {
        Timer tt = new Timer();//定时类
        tt.schedule(new TimerTask() {//创建一个定时任务
            @Override
            public void run() {
                long nd = System.currentTimeMillis();//获取系统时间
                Iterator<Map.Entry<String, Long>> entries = keytime.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<String, Object> entry = (Map.Entry) entries.next();
                    String key = entry.getKey(); //获取key
                    long value = (Long) entry.getValue(); //获取value
                    long rt = nd - value;//获取当前时间跟存入时间的差值
                    if (rt > EXPIRATIONTIME) {//判断时间是否已经过期  如果过期则清楚key 否则不做处理
                        statmentMap.remove(key);
                        entries.remove();
                        System.out.println("prepareStatment of table Key:" + key + " 已过期  清空");
                    }
                }
            }
        }, START, INTERVAL);//从0秒开始，每隔10秒执行一次
    }
}

