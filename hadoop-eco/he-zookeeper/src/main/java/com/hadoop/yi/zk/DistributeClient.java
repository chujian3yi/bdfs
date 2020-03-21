package com.hadoop.yi.zk;

import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * zk 监听服务器节点客户端，监听到服务端有服务节点下线，进行业务操作
 */
public class DistributeClient {

    /**
     * zk server connectString
     */
    public static String connectString = "192.168.12.151:2181" +
            ",192.168.12.152:2181" +
            ",192.168.12.153";
    public static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;


    /**
     * 创建 zk客户端连接
     */
    public void getConnect() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
            //再次启动监听器
            try {
                getServerList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 获取服务器列表信息
     */
    public void getServerList() throws Exception {

        String parentNode = "/servers";
        //1. 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode, true);
        //2. 存储服务器信息列表
        ArrayList<Object> servers = new ArrayList<>();
        //3. 便利所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));

        }
        //4. 打印服务器列表信息
        System.out.println(servers);

    }

    /**
     * 业务功能
     */
    public void business() throws Exception {
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //1.获取连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        //2.获取 servers 的子节点信息，从中获取服务器信息列表
        client.getServerList();
        //3.业务进程启动（重新注册服务器，进行上线或者其她通知服务）
        client.business();
    }
}
