package org.linc.spark.SparkSQLConsole;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;

/**
 * 全局系统变量
 */
public class GlobalVar {
    public static HashMap<String, String> configMap = new HashMap<String, String>();

    /**
     * 默认配置值
     */
    static {
        configMap.put("resource.url", "http://10.4.21.103:8080/SparkSQLServer/service/");
        configMap.put("sql.result.itemsPerPage", "5");
    }

    public static void parseArgs(String args[]) {
        // 解析程序运行参数
        ArgumentParser parser = ArgumentParsers.newArgumentParser("console").defaultHelp(true).description("Spark SQL Console By LINC");
        parser.addArgument("-c", "--config")
                .setDefault("./conf.xml")
                .help("specify the system configuration file");

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        String configureFilePath = ns.getString("config");
        try {
            GlobalVar.readConFile(configureFilePath);
        } catch (Exception e) {
            System.err.println("解析配置文件失败，失败原因：" + e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * 读取配置文件
     *
     * @param filePath 配置文件路径
     * @throws ConfigurationException 读取 XML 配置文件失败
     */
    private static void readConFile(String filePath) throws ConfigurationException {
        XMLConfiguration config = new XMLConfiguration(filePath);
        NodeList list = config.getDocument().getElementsByTagName("entry");
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            String key = node.getAttributes().getNamedItem("key").getTextContent();
            String val = node.getTextContent();
            configMap.put(key, val);
        }
        System.out.println(configMap);
    }
}