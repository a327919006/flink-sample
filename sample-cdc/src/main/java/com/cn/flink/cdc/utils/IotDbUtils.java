package com.cn.flink.cdc.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.*;
import java.text.MessageFormat;

@Slf4j
public class IotDbUtils {

    /**
     * 获取站点的电价与服务费
     *
     * @param conn mysql连接
     * @return 物模型json串
     */
    public static String getProductModel(ParameterTool parameterTool, Connection conn, String productKey) {
        log.info("getProductModel = {}", productKey);
        try {
            if (conn == null || conn.isClosed()) {
                conn = DriverManager.getConnection(parameterTool.get("mysql.url"), parameterTool.get("mysql.username"), parameterTool.get("mysql.password"));
            }
            String sql = MessageFormat.format("SELECT physical_content " +
                    "FROM iot_physical_model m " +
                    "INNER JOIN iot_product p " +
                    "ON p.id = m.product_id " +
                    "WHERE p.product_key = {0} " +
                    "AND m.is_deleted = 0 " +
                    "AND p.is_deleted = 0 " +
                    "LIMIT 1", productKey);
            try (PreparedStatement pst = conn.prepareStatement(sql)) {
                ResultSet resultSet = pst.executeQuery();
                while (resultSet.next()) {
                    return resultSet.getString("physical_content");
                }
            }
        } catch (Exception e) {
            log.error("mysql断开连接", e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return "";
    }

    /**
     * 获取属性类型
     */
    public static String getPropertyType(String model, String identifier, String key) {
        if (StringUtils.isEmpty(model) || !JSONObject.isValid(model)) {
            return getDefaultPropertyType(identifier);
        }
        JSONObject modelObj = JSONObject.parseObject(model);
        if (!modelObj.containsKey("properties")) {
            return getDefaultPropertyType(identifier);
        }
        JSONArray types = modelObj.getJSONArray("properties");
        for (Object type : types) {
            JSONObject typeObj = (JSONObject) type;
            String name = typeObj.getString("identifier");
            if (StringUtils.equalsIgnoreCase(name, key)) {
                return typeObj.getJSONObject("dataType").getString("type");
            }
        }
        return getDefaultPropertyType(identifier);
    }

    private static String getDefaultPropertyType(String identifier) {
        return StringUtils.equalsIgnoreCase("session", identifier) ? "TEXT" : "DOUBLE";
    }

}
