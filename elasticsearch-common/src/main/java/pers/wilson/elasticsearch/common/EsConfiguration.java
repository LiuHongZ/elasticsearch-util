package pers.wilson.elasticsearch.common;

import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class EsConfiguration {

    private static final String ELASTICSEARCH_YML = "elasticsearch.yml";
    public static final String CLUSTER_NAME;
    public static final String CLUSTER_NODES;
    public static final List<String> CLUSTER_HOSTS;
    public static final String INDEX_NAME;
    public static final String ELASTICSEARCH_INDEX_TYPE;

    static {
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(ELASTICSEARCH_YML);
            if (is == null) {
                is = EsConfiguration.class.getResourceAsStream("/" + ELASTICSEARCH_YML);
                if (is == null) {
                    is = new BufferedInputStream(new FileInputStream(System.getProperty("user.dir") +
                            File.separator + "conf" + "/" + ELASTICSEARCH_YML));
                }
            }
            Yaml yaml = new Yaml();
            String result = yaml.dumpAsMap(yaml.load(is));

            int index = result.indexOf("cluster-name:") + "cluster-name:".length() + 1;
            CLUSTER_NAME = result.substring(index, result.indexOf("\n", index)).trim();

            index = result.indexOf("cluster-nodes") + "cluster-nodes".length() + 1;
            CLUSTER_NODES = result.substring(index, result.indexOf("\n", index)).trim();

            index = result.indexOf("cluster-hosts") + "cluster-hosts".length() + 1;
            CLUSTER_HOSTS = Arrays.asList(result.substring(index, result.indexOf("\n", index)).trim().split(","));

            index = result.indexOf("index-name") + "index-name".length() + 1;
            INDEX_NAME = result.substring(index, result.indexOf("\n", index)).trim();

            index = result.indexOf("index-type") + "index-type".length() + 1;
            ELASTICSEARCH_INDEX_TYPE = result.substring(index, result.indexOf("\n", index)).trim();

        } catch (IOException e) {
            log.error("系统配置文件找不到.......", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("配置参数错误.......", e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {
                log.error("输入流关闭错误", e);
            }
        }
    }
}
