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
                // IDE中输入与输出
                Scanner sc = new Scanner(System.in);
                System.out.print("Please input your name: ");
                String name = sc.nextLine();
                System.out.print("Please input your password: ");
                String password = sc.nextLine();

               // 控制台中输入与输出
//              java.io.Console cons = System.console();
//              String name=null,password=null;
//              if (cons != null)
//              {              // 判断是否有控制台的使用权
//                  name = new String(cons.readLine("Please input your name: "));      // 读取整行字符
//                  password = new String(cons.readPassword("Please input your password: "));   // 读取密码,输入时不显示
//              }

                // 连接服务器验证用户名与密码
                NetworkInterface net = new NetworkInterface();

                // 对字符串进行utf-8编码为url格式
                String geturl = "check?name=" + URLEncoder.encode(name, "utf-8") + "&password=" + password;
                String state = net.execute(geturl);

                if (state.equals("ok"))
                // 用户名，密码正确
                {
                    System.out.println("登录成功");

                    // 继续输入sql语句等
                    String lines;
                    System.out.print("SQL> ");
                    while (!(lines = sc.nextLine()).equals("quit"))//如果不输入quit则一直输入
                    {
                        if(lines.isEmpty()) {
                            System.out.print("SQL> ");
                            continue;
                        }
                        String json = net.execute(URLEncoder.encode(lines, "utf-8"));
                        /* json 数据解析并打印 */
                        Console console = new Console();
                        console.jsonparser(json);

                        System.out.print("SQL> ");
                    }
                    break;
                } else if (state.equals("no"))
                // 用户名，密码不正确
                {
                    System.out.println("用户名与密码不匹配，请再次输入");
                    // 再次输入用户名密码
                }
                else {
                    // 出现问题（如传输过程等问题）
                    System.out.println(state);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }


    /**
     * 将获得的json数据进行解析，并显示在界面上
     *
     * @param     json  json字符串
     */
    private void jsonparser(String json){
        String code,msg,time,size;
        JSONObject jsonObj;
        try {
            // 获取整个json字符串对象
            jsonObj = JSONObject.fromObject(json);

            code = jsonObj.getString("code");
            msg = jsonObj.getString("msg");
            time = jsonObj.getString("time");
            size = jsonObj.getString("size");

        }catch (Exception e)
        {
            System.out.println("json数据解析出错");
            return;
        }

        if(Integer.valueOf(code) == 0) {
            // 获取字符串对象中的result数组
            JSONArray jsonArray = jsonObj.getJSONArray("result");
            if (null != jsonArray && jsonArray.size() > 0) {
                List<Map<String, Object>> mapListJson = (List) jsonArray;

                // 得到表头
                ArrayList<String> headerList = new ArrayList<String>();
                Map<String, Object> obj1 = mapListJson.get(0);

                // TODO: 需要优化
                for (Map.Entry<String, Object> entry : obj1.entrySet()) {
                    String strkey1 = entry.getKey();    // 表头
                    headerList.add(strkey1);
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

                /* 生成TextTable */
                TextTable tt = new TextTable(header, data);
                // this adds the numbering on the left
                tt.setAddRowNumbering(true);
                // sort by the first column
                // tt.setSort(0);

                /* Print table */
                tt.printTable();
                System.out.println();
                System.out.println("SQL Execution Time: " + time + "  HDFS R&W Size: " + size);
            }
        }
        else{
            System.out.println(msg);
        }
    }
}
