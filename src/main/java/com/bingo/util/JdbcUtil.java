package com.bingo.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class JdbcUtil {


    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        //创建集合.用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建泛型对象
            T t = clz.newInstance();

            //给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转化为驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //获取列值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t,columnName,value);


            }

            //将该对象添加集合
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();

        return resultList;

    }

    public void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://172.30.12.108:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=i@SH021#bg");
        List<JSONObject> queryList = queryList(connection, "select * from flinkConfig", JSONObject.class, false);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
            String proid = jsonObject.getString("proid");
            String mmac = jsonObject.getString("mmac");
        }

        connection.close();
    }

}

