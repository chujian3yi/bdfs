package com.hadoop.yi.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 监听服务器节点动态上线
 * 本案例前提：已经在集群上创建/servers 节点 create /servers "servers" Create /servers
 */
public class DistributeServer {

    /**
     * zk server connectString
     */
    public static String connectString = "192.168.12.151:2181" +
            ",192.168.12.152:2181" +
            ",192.168.12.153";
    public static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    /**
     * 创建zk的客户端连接
     */
    public void getConnect() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
            // 收到事件通知时回调函数
            System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
        });
    }

    /**
     * 注册服务器
     */
    public void registServer(String hostname) throws Exception {
        String parentNode = "/servers";
        String create = zkClient.create(
                parentNode + "/server",
                hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online " + create);
    }

    /**
     * 业务功能
     */
    public void business(String hostname) {
        System.out.println(hostname + "is working ...");
    }


    public static void main(String[] args) throws Exception {
        //1.获取连接
        DistributeServer server = new DistributeServer();
        server.getConnect();
        //2.利用zk连接注册服务器信息
        server.registServer(args[0]);
        //3.启动业务功能
        server.business(args[0]);
    }
}
