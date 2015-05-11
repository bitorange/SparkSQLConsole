package org.linc.spark.SparkSQLConsole;

/**
 * Created by orange on 2015/3/30.
 */

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import dnl.utils.text.table.TextTable;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * 程序入口
 */
public class ApplicationEntry {
    private static String resourceURL = null;

    /**
     * 解析程序运行参数，读取配置文件
     *
     * @param args 程序参数
     */
    private static void readConfigureFile(String[] args) {
        GlobalVar.parseArgs(args);
        ApplicationEntry.resourceURL = GlobalVar.configMap.get("resource.url");
    }

    /**
     * main 函数
     *
     * @param args 程序参数
     */
    public static void main(String[] args) {
        // 解析程序运行参数，读取配置文件
        ApplicationEntry.readConfigureFile(args);

        Client client = null;
        while (true) {
            try {
                // 用户名与密码
                Scanner sc = new Scanner(System.in);
                System.out.print("用户名: ");
                String name = sc.nextLine();
                System.out.print("密码: ");
                String password = sc.nextLine();

                // 对字符串进行 utf-8 编码为 url 格式
                String getURL = "check?name=" + URLEncoder.encode(name, "utf-8") + "&password=" + password;

                // 连接到远程服务器
                String state;
                String myJsonResponse;
                try {
                    if (client == null) {
                        client = Client.create();
                    }

                    // 连接服务器验证用户名与密码
                    WebResource webResource = client
                            .resource(ApplicationEntry.resourceURL + getURL);

                    ClientResponse response = webResource.accept("application/json")
                            .get(ClientResponse.class);

                    if (response.getStatus() != 200) {
                        throw new RuntimeException("Failed : HTTP error code : "
                                + response.getStatus());
                    }

                    myJsonResponse = response.getEntity(String.class);
                } catch (Exception e) {
                    System.out.println("Err: 连接远程服务器的过程中发生错误，错误原因：" + e.getMessage());
                    continue;
                }

                // JSON 解析，获取状态信息
                try {
                    JSONObject jsonObject = JSONObject.fromObject(myJsonResponse);
                    state = jsonObject.getString("msg");
                } catch (Exception e) {
                    System.out.println("Error: JSON 数据解析出错，服务器端未传回指定格式数据");
                    continue;
                }

                if (state.equals("ok"))
                // 用户名，密码正确
                {
                    System.out.println("登录成功");

                    // 继续输入 SQL 语句
                    String lines;
                    System.out.print("SQL> ");
                    while (!(lines = sc.nextLine()).equals("quit")) // 如果不输入 quit 则一直输入
                    {
                        if (lines.isEmpty()) {
                            System.out.print("SQL> ");
                            continue;
                        }

                        /* 对末尾 ";" 和空格处理 */
                        lines = rightTrim(lines);
                        if (lines.lastIndexOf(';') == lines.length() - 1) {
                            lines = lines.substring(0, lines.length() - 1);

                        }

                        lines = rightTrim(lines);
                        if (lines.equals("")) {
                            System.out.print("SQL> ");
                            continue;
                        }

                        try {
                            // 执行 SQL 语句，获得结果
                            lines = URLEncoder.encode(lines, "utf-8");
                            lines = "sqlExecute?sql=" + lines;
                            WebResource webResource = client
                                    .resource(ApplicationEntry.resourceURL + lines);

                            ClientResponse response = webResource.accept("application/json")
                                    .get(ClientResponse.class);

                            if (response.getStatus() != 200) {
                                throw new RuntimeException("Failed : HTTP error code : "
                                        + response.getStatus());
                            }

                            String json = response.getEntity(String.class);

                            // JSON 数据解析并打印
                            ApplicationEntry myConsole = new ApplicationEntry();
                            myConsole.jsonParser(json);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        System.out.print("SQL> ");
                    }
                    break;
                }
                // 用户名，密码不正确
                else if (state.equals("no")) {
                    // 再次输入用户名密码
                    System.out.println("Err: 账号密码错误，请重试");
                } else {
                    // 服务器端出现问题（如传输过程等问题）
                    System.out.println("Error: " + state);
                }
            } catch (Exception e) {
                System.out.println("Err: 发生错误，错误原因：" + e.getMessage());
            }
        }
    }

    /**
     * 去掉末尾空格
     *
     * @param str 需要处理的字符串
     * @return 处理得到的字符串
     */
    public static String rightTrim(String str) {
        return str.replaceAll("\\s+$", "");
    }

    /**
     * 将获得的json数据进行解析，并显示在界面上
     *
     * @param json json字符串
     */
    private void jsonParser(String json) {
        String code, msg, time, size;
        JSONObject jsonObj;

        // 解析 JSON 数据
        try {
            // 获取code对象
            jsonObj = JSONObject.fromObject(json);
            code = jsonObj.getString("code");

        } catch (Exception e) {
            System.out.println("Error: JSON 数据解析出错，服务器端未传回指定格式数据");
            return;
        }

        // 检查错误代码
        if (Integer.valueOf(code) == 0) {   // 如果无错误
            time = jsonObj.getString("time");
            size = jsonObj.getString("size");

            // 获取字符串对象中的 result 数据
            JSONArray jsonArray = jsonObj.getJSONArray("result");
            if (null != jsonArray && jsonArray.size() > 0) {
                List<Map<String, Object>> mapListJson = (List) jsonArray;

                // 得到表头
                ArrayList<String> headerList = new ArrayList<String>();
                Map<String, Object> obj1 = mapListJson.get(0);

                // TODO: 需要优化
                for (Map.Entry<String, Object> entry : obj1.entrySet()) {
                    String strKey = entry.getKey();    // 表头
                    headerList.add(strKey);
                }

                // 表头数组
                String[] header = new String[headerList.size()];
                header = headerList.toArray(header);

                // 得到表中数据内容存在二维数组中
                String[][] data = new String[mapListJson.size()][headerList.size()];
                int j;
                for (int i = 0; i < mapListJson.size(); i++) {
                    Map<String, Object> obj = mapListJson.get(i);
                    j = 0;
                    for (Map.Entry<String, Object> entry : obj.entrySet()) {
                        Object strval1 = entry.getValue();
                        data[i][j++] = strval1.toString();
                    }
                }

                // 生成 TextTable
                TextTable tt = new TextTable(header, data);

                // 表格左侧加上行号
                tt.setAddRowNumbering(true);

                // 打印表格
                tt.printTable();
                System.out.println();
                System.out.println("SQL Execution Time: " + time + "  HDFS R&W Size: " + size);
            }
        } else {
            msg = jsonObj.getString("msg");
            System.out.println("Error: " + msg);
        }
    }
}