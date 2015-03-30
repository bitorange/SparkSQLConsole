package org.linc.spark.SparkSQLConsole;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by orange on 2015/3/16.
 */
public class NetworkInterface {
    private static final String targetURL = "http://localhost:8080/SparkSQLServer/service/";

    public String execute(String geturl) throws Exception {
        String lines;
        try {
            URL targetUrl = new URL(targetURL + geturl);

            // 返回不同的URLConnection子类的对象，这里URL是一个http，因此实际返回的是HttpURLConnection
            HttpURLConnection httpConnection = (HttpURLConnection) targetUrl.openConnection();

            // 进行连接，但是实际上get request要在下一句的httpConnection.getInputStream()函数中才会真正发到服务器
            httpConnection.connect();

            // 取得输入流，并使用Reader读取
            BufferedReader reader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));

            // 获取的输入流肯定只有一行
            lines = reader.readLine();
            reader.close();

            // 断开连接
            httpConnection.disconnect();
        }catch (Exception e) {
            throw e;
        }

        return lines;
    }

}
