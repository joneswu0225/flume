package com.datayes.dataifs.applog.flume.sinks.mysql;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.datayes.dataifs.applog.flume.utils.Constant;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



@Slf4j
public class MysqlSink extends AbstractSink implements Configurable {
    private String hostname;
    private String port;
    private String databaseName;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;
    private List<String> columnNames;

    private static final String CONNURL = "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&pinGlobalTxToPhysicalConnection=true&autoReconnect=true";
    private static final String SELECTSQL = "select * from %s limit 0";
    private static final String INSERTSQL = "insert into %s (%s) values (%s)";
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    public MysqlSink () {
        log.info("MysqlSink start...");
    }


    @Override
    public void configure(Context context) {
        hostname = context.getString(Constant.HOSTNAME);
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
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
    }
    @Override
    public void start () {
        log.info("start method coming ");
        super.start();
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            log.error("ClassNotFountException", e);
        }

    }


    @Override
    public void stop() {
        log.info("stop method coming ");
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.error("SQLException ", e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("SQLException ", e);
            }
        }
    }

    @Override
    public Status process() {


        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        String tableName = "";

        transaction.begin();
        try {
            List<Map<String, String>> resultList = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {//对事件进行处理

                    content = new String(event.getBody(),"utf-8");

                    JSONObject jsonObject = JSONObject.parseObject(content);
                    JSONObject commonJson = jsonObject.getJSONObject("common");
                    JSONObject eventJson = jsonObject.getJSONObject("event");

                    Map<String,String> resultMap = new HashMap<>();
                    String appId = commonJson.getString("appId");
                    String appEnv = commonJson.getString("appEnv");
                    tableName = "applog_%s";
                    if (StringUtils.isNotBlank(appId)) {
                        tableName = String.format(tableName, appId);
                        tableName += (StringUtils.isNotBlank(appEnv) ? ("_" + appEnv) : "");
                        resultMap.put("tableName", tableName);

                    } else {
                        continue;
                    }

                    Map<String, String> commonMap = JSONObject.parseObject(commonJson.toJSONString(), new TypeReference<Map<String, String>>(){});
                    Map<String, String> eventMap = JSONObject.parseObject(eventJson.toJSONString(), new TypeReference<Map<String, String>>(){});

                    resultMap.putAll(commonMap);
                    resultMap.putAll(eventMap);

                    resultMap.put("appeartime", stampToDate(eventJson.getString("timestamp")));


                    resultMap.put("detail", eventJson.toJSONString());
                    resultMap.put("common", commonJson.toJSONString());


                    resultList.add(resultMap);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            transaction.commit();



            if (CollectionUtils.isNotEmpty(resultList)) {
                log.info("tableName=" + resultList.get(0).get("tableName"));
                createPrepareStatement(resultList.get(0).get("tableName"));
                preparedStatement.clearBatch();
                for (Map<String, String> temp : resultList) {

                    for (int i = 0; i < columnNames.size(); i++) {
                        preparedStatement.setString(i+1, temp.get(columnNames.get(i)));
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                conn.commit();
            }

        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                log.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            log.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }
        return result;
    }

    private void createPrepareStatement (String tableName) {
        String url = String.format(CONNURL, hostname, port, databaseName);

        //调用DriverManager对象的getConnection()方法，获得一个Connection对象
        columnNames = new ArrayList<>();
        String tableSql = String.format(SELECTSQL,tableName);
        try {
            //mysql连接
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //获取字段名
            preparedStatement = conn.prepareStatement(tableSql);
            ResultSetMetaData rsmd = preparedStatement.getMetaData();
            int size = rsmd.getColumnCount();
            StringBuilder placeholders = new StringBuilder("?");//占位符
            for (int i = 1; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i+1));
            }
            for (int i = 2; i < size; i++) {
                placeholders.append(",?");
            }
            String columns = Joiner.on(",").join(columnNames);

            String insertSql = String.format(INSERTSQL, tableName, columns, placeholders);

            //创建一个Statement对象
            preparedStatement = conn.prepareStatement(insertSql);
            log.info("MysqlSink insertSql: {}", insertSql);
        } catch (SQLException e) {
            log.error("SQLException ", e);
            System.exit(1);
        }
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

}
