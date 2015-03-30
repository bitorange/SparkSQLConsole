package org.linc.spark.SparkSQLConsole;

/**
 * Created by orange on 2015/3/16.
 */
import dnl.utils.text.table.TextTable;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Console {
    public static void main(String[] args) {
        while (true) {
            try {
                // IDE 中输入与输出
                Scanner sc = new Scanner(System.in);
                System.out.print("用户名: ");
                String name = sc.nextLine();
                System.out.print("密码: ");
                String password = sc.nextLine();

                // 连接服务器验证用户名与密码
                NetworkInterface net = new NetworkInterface();

                // 对字符串进行 utf-8 编码为 url 格式
                String getURL = "check?name=" + URLEncoder.encode(name, "utf-8") + "&password=" + password;

                // 连接到远程服务器
                String state;
                String myJsonResponse;
                try{
                    myJsonResponse = net.execute(getURL);
                }
                catch  (Exception e) {
                    System.out.println("Err: 连接远程服务器的过程中发生错误，错误原因：" + e.getMessage());
                    continue;
                }

                // JSON 解析，获取状态信息
                try {
                    JSONObject jsonObject = JSONObject.fromObject(myJsonResponse);
                    state = jsonObject.getString("msg");
                }
                catch (Exception e){
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
                        if(lines.isEmpty()) {
                            System.out.print("SQL> ");
                            continue;
                        }

                        // 末尾 ";" 和空格处理
                        lines = rightTrim(lines);
                        if (lines.lastIndexOf(';') == lines.length() - 1) {
                            lines = lines.substring(0, lines.length() - 1);

                        }

                        lines = rightTrim(lines);
                        if(lines.equals("")){
                            System.out.print("SQL> ");
                            continue;
                        }

                        // 执行 SQL 语句，获得结果
                        String json = net.execute(URLEncoder.encode(lines, "utf-8"));

                        // JSON 数据解析并打印
                        Console console = new Console();
                        console.jsonParser(json);

                        System.out.print("SQL> ");
                    }
                    break;
                } else if (state.equals("no"))
                // 用户名，密码不正确
                {
                    // 再次输入用户名密码
                    System.out.println("Err: 账号密码错误，请重试");
                }
                else {
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
     * @param str 需要处理的字符串
     * @return 处理得到的字符串
     */
    public static String rightTrim(String str) {
        return str.replaceAll("\\s+$", "");
    }

    /**
     * 将获得的json数据进行解析，并显示在界面上
     *
     * @param     json  json字符串
     */
    private void jsonParser(String json){
        String code, msg, time, size;
        JSONObject jsonObj;

        // 解析 JSON 数据
        try {
            // 获取整个json字符串对象
            jsonObj = JSONObject.fromObject(json);
            code = jsonObj.getString("code");
            msg = jsonObj.getString("msg");
            time = jsonObj.getString("time");
            size = jsonObj.getString("size");
        }catch (Exception e)
        {
            System.out.println("Error: JSON 数据解析出错，服务器端未传回指定格式数据");
            return;
        }

        // 检查错误代码
        if(Integer.valueOf(code) == 0) {
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
        }
        else{
            System.out.println("Error: " + msg);
        }
    }
}
