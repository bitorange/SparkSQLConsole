package org.linc.spark.SparkSQLConsole;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import dnl.utils.text.table.TextTable;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.glassfish.jersey.SslConfigurator;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * 程序入口
 *
 * @author bitorange / ihainan
 * @version 1.1
 */
public class ApplicationEntry {
    private static String resourceURL = null;
    private static int numberOfItemsPerPage = 2;
    private static SSLContext sslContext;
    private static String clientTrustCer, clientTrustCerPwd, protocol;
    private static boolean isEnableSSL;


    /**
     * 解析程序运行参数，读取配置文件
     *
     * @param args 程序参数
     */
    private static void readConfigureFile(String[] args) {
        GlobalVar.parseArgs(args);
        ApplicationEntry.resourceURL = GlobalVar.configMap.get("resource.url");
        ApplicationEntry.numberOfItemsPerPage = Integer.valueOf(GlobalVar.configMap.get("sql.result.itemsPerPage"));
        ApplicationEntry.clientTrustCer = GlobalVar.configMap.get("ssl.cer.clientTrustCer");
        ApplicationEntry.clientTrustCerPwd = GlobalVar.configMap.get("ssl.cer.clientTrustCerPwd");
        ApplicationEntry.protocol = GlobalVar.configMap.get("ssl.cer.protocol");
        ApplicationEntry.isEnableSSL = Boolean.valueOf(GlobalVar.configMap.get("ssl.enableSSL"));
    }

    private static void setUpSSL(){
        SslConfigurator sslConfig = SslConfigurator.newInstance();
        sslConfig.trustStoreFile(ApplicationEntry.clientTrustCer).trustStorePassword(ApplicationEntry.clientTrustCerPwd);
        sslConfig.securityProtocol(ApplicationEntry.protocol);
        ApplicationEntry.sslContext = sslConfig.createSSLContext();

    }

    /**
     * 主机名认证，本地直接通过验证
     */
    private static class MyHostnameVerifier implements HostnameVerifier {
        private static String getHost(String url){
            if(url == null || url.length() == 0)
                return "";

            int doubleslash = url.indexOf("//");
            if(doubleslash == -1)
                doubleslash = 0;
            else
                doubleslash += 2;

            int end = url.indexOf('/', doubleslash);
            end = end >= 0 ? end : url.length();

            int port = url.indexOf(':', doubleslash);
            end = (port > 0 && port < end) ? port : end;

            return url.substring(doubleslash, end);
        }

        @Override
        public boolean verify(String hostname, SSLSession sslSession) {
            String hostnameFromUrl = getHost(ApplicationEntry.resourceURL);
            if("127.0.0.1".equals(hostname) || "localhost".equals("hostname") || hostnameFromUrl.equals(hostname)){
                return true;
            }
            else{
                return false;
            }
        }
    }

    /**
     * main 函数
     *
     * @param args 程序参数
     */
    public static void main(String[] args) {
        // 解析程序运行参数，读取配置文件
        ApplicationEntry.readConfigureFile(args);
        if(isEnableSSL) {
            ApplicationEntry.setUpSSL();
        }

        Client client = null;
        while (true) {

            // 用户名与密码
            Scanner sc = new Scanner(System.in);
            System.out.print("UserName : ");
            String name = sc.nextLine();
            System.out.print("Password : ");
            String password = sc.nextLine();

            // 对字符串进行 utf-8 编码为 url 格式
            String getURL = null;
            try {
                getURL = "check?name=" + URLEncoder.encode(name, "utf-8") + "&password=" + password;
            } catch (UnsupportedEncodingException e) {
                System.err.println("Error occurred during encoding username & password: \n" + e.getMessage());
            }

            // 连接到远程服务器
            String state;
            String myJsonResponse;
            try {
                if (client == null) {
                    if(isEnableSSL) {
                        ClientConfig cc = new DefaultClientConfig();
                        cc.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                                new HTTPSProperties(new MyHostnameVerifier(), sslContext));
                        client = Client.create(cc);
                    }
                    else{
                        client = Client.create();
                    }
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
                System.err.println("Error occurred during connecting to the remote server: \n" + e.getMessage());
                continue;
            }

            // JSON 解析，获取状态信息
            try {
                JSONObject jsonObject = JSONObject.fromObject(myJsonResponse);
                state = jsonObject.getString("msg");
            } catch (Exception e) {
                System.err.println("Error occurred during the parsing of JSON string: \n" + e.getMessage());
                continue;
            }

            String lastJsonData = "";   // 最近一次 SQL 查询得到的查询数据
            int startPoint = 0;
            int endPoint;
            int size = -1;

            if (state.equals("ok"))
            // 用户名，密码正确
            {
                System.out.println("Login succeeded");

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
                    lines = lines.replace("\"", "'");
                    lines = rightTrim(lines);
                    if (lines.lastIndexOf(';') == lines.length() - 1) {
                        lines = lines.substring(0, lines.length() - 1);
                    }

                    lines = rightTrim(lines);
                    if (lines.equals("")) {
                        System.out.print("SQL> ");
                        continue;
                    }

                    // 检查是不是指令 "next"
                    if (lines.equalsIgnoreCase("next")) {
                        if (lastJsonData.equals("")) {
                            System.out.println("No saved SQL result.");
                        } else {
                            startPoint = startPoint + numberOfItemsPerPage >= size ? startPoint : startPoint + numberOfItemsPerPage;
                            endPoint = startPoint + numberOfItemsPerPage - 1 >= size ? size - 1 : startPoint + numberOfItemsPerPage - 1;
                            ApplicationEntry myConsole = new ApplicationEntry();
                            size = myConsole.jsonParser(lastJsonData, startPoint, endPoint);
                        }
                    }
                    // 检查是不是指令 "pre"
                    else if (lines.equalsIgnoreCase("pre")) {
                        if (lastJsonData.equals("")) {
                            System.out.println("No saved SQL result.");
                        } else {
                            startPoint = startPoint - numberOfItemsPerPage < 0 ? 0 : startPoint - numberOfItemsPerPage;
                            endPoint = startPoint + numberOfItemsPerPage - 1 >= size ? size - 1 : startPoint + numberOfItemsPerPage - 1;
                            ApplicationEntry myConsole = new ApplicationEntry();
                            size = myConsole.jsonParser(lastJsonData, startPoint, endPoint);
                        }
                    } else {
                        // 执行 SQL 语句，获得结果
                        try {
                            lines = URLEncoder.encode(lines, "utf-8");
                        } catch (UnsupportedEncodingException e) {
                            System.err.println("Error occurred during encoding username & password: \n" + e.getMessage());
                        }
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
                        lastJsonData = json;
                        startPoint = 0;
                        endPoint = numberOfItemsPerPage - 1;
                        size = myConsole.jsonParser(json, startPoint, endPoint);
                    }
                    System.out.print("SQL> ");
                }
                break;
            }
            // 用户名，密码不正确
            else if (state.equals("no")) {
                // 再次输入用户名密码
                System.err.println("Wrong password, please try again.");
            } else {
                // 服务器端出现问题（如传输过程等问题）
                System.err.println("ERROR occurred during connecting to the remote server\n" + state);
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
    private int jsonParser(String json, int startPoint, int endPoint) {
        String code, msg, time, size;
        JSONObject jsonObj;

        // 解析 JSON 数据
        try {
            // 获取错误代码
            jsonObj = JSONObject.fromObject(json);
            code = jsonObj.getString("code");
        } catch (Exception e) {
            System.out.println("Error occurred during the parsing of JSON string: \n" + e.toString());
            return -1;
        }

        // 检查错误代码
        if (Integer.valueOf(code) == 0) {   // 执行命令正常
            time = jsonObj.getString("time");
            size = jsonObj.getString("size");

            // 获取字符串对象中的 result 数据
            JSONArray jsonArray = jsonObj.getJSONArray("result");
            if (null != jsonArray && jsonArray.size() > 0) {
                List<Map<String, Object>> mapListJson = (List) jsonArray;

                // 得到表头
                ArrayList<String> headerList = new ArrayList<String>();
                Map<String, Object> jsonObjs = mapListJson.get(0);

                for (Map.Entry<String, Object> entry : jsonObjs.entrySet()) {
                    String strKey = entry.getKey();
                    headerList.add(strKey);
                }

                // 表头数组
                String[] header = new String[headerList.size()];
                header = headerList.toArray(header);

                // 得到表中数据内容存在二维数组中
                if (endPoint >= mapListJson.size()) {
                    endPoint = mapListJson.size() - 1;
                }
                String[][] data = new String[endPoint - startPoint + 1][headerList.size()];
                int j;
                for (int i = 0; i < endPoint - startPoint + 1; i++) {
                    Map<String, Object> obj = mapListJson.get(startPoint + i);
                    j = 0;
                    for (Map.Entry<String, Object> entry : obj.entrySet()) {
                        Object strVal = entry.getValue();
                        data[i][j++] = strVal.toString();
                    }
                }

                // 生成 TextTable
                TextTable tt = new TextTable(header, data);

                // 表格左侧加上行号
                tt.setAddRowNumbering(true);

                // 打印表格
                tt.printTable();
                System.out.println();
                System.out.println(mapListJson.size() + " rows [" + startPoint + " - " + endPoint + "] in set (" + time + ", " + size + ")");
                if (endPoint != mapListJson.size() - 1) {
                    System.out.println("Enter command `next` to see next " + numberOfItemsPerPage + " row" + (numberOfItemsPerPage == 1 ? "." : "s."));
                }
                if (startPoint != 0) {
                    System.out.println("Enter command `pre` to see previous " + numberOfItemsPerPage + " row" + (numberOfItemsPerPage == 1 ? "." : "s."));
                }
                return mapListJson.size();
            } else {
                System.out.println("Query OK (" + time + ", " + size + ")");
                return 0;
            }
        } else {
            msg = jsonObj.getString("msg");
            System.err.println("Error occurred during the execution of SQL command: \n" + msg);
            return -1;
        }
    }
}