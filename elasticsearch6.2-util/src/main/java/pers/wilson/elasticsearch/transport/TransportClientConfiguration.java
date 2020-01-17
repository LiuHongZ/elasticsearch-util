package pers.wilson.elasticsearch.transport;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import static pers.wilson.elasticsearch.common.EsConfiguration.CLUSTER_NAME;
import static pers.wilson.elasticsearch.common.EsConfiguration.CLUSTER_NODES;

@Slf4j
public class TransportClientConfiguration {

    public static final TransportClient CLIENT;

    static {
        CLIENT = getTransportClient();
    }

    public static TransportClient getTransportClient() {
        ArrayList<TransportAddress> transportAddresses = new ArrayList<>();
        for (String clusterNode : CLUSTER_NODES.split(",")) {
            try {
                String[] split = clusterNode.trim().split(":");
                transportAddresses.add(new TransportAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1])));
            } catch (UnknownHostException e) {
                log.error("IP地址错误", e);
            }
        }
        log.info("开始创建TransportClient...");
        Settings settings = Settings.builder()
                .put("cluster.name", CLUSTER_NAME)
                .put("client.transport.sniff", false)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        transportAddresses.forEach(client::addTransportAddress);
        log.info("创建TransportClient成功!");
        return client;
    }
}
