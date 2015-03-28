import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by orange on 2015/3/16.
 */
public class NetworkInterface {
    private static final String targetURL = "http://localhost:8080/SparkSQLServer/service/";
//    private static final String targetURL = "http://localhost:8080/MyServer/service/";
//    HttpURLConnection httpConnection;

    public String execute(String geturl) {
        String lines = null;
        try {
            URL targetUrl = new URL(targetURL + geturl);

            /* 返回不同的URLConnection子类的对象，这里URL是一个http，因此实际返回的是HttpURLConnection */
            //     HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
            HttpURLConnection httpConnection = (HttpURLConnection) targetUrl.openConnection();

            // 进行连接，但是实际上get request要在下一句的httpConnection.getInputStream()函数中才会真正发到服务器
            httpConnection.connect();

            // 取得输入流，并使用Reader读取
            BufferedReader reader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));

            //获取的输入流肯定只有一行

            lines = reader.readLine();

//            while ((lines = reader.readLine()) != null) {
//                System.out.println(lines);
//            }
            reader.close();

            // 断开连接
            httpConnection.disconnect();


        } catch (MalformedURLException e) {
            lines=e.getMessage();
        } catch (ConnectException e) {
            // 连接到服务器出错
            // lines = e.getMessage();
            lines = "连接到服务器出错";
        } catch (IOException e) {
            lines=e.getMessage();
        }catch (Exception e) {
            lines=e.getMessage();
        }

        return lines;
    }

}
