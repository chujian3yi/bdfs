package com.hadoop.yi.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 创建 Zookeeper 客户端
 */
public class CreateZkClient {
    /** zk server connectString*/
    public static String connectString = "192.168.12.151:2181" +
            ",192.168.12.152:2181" +
            ",192.168.12.153";
    public static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    /** 初始化zkClient*/
    @Before
    public void init() throws Exception{
        // connectString, sessionTimeout, new Watcher() 构造函数构建 zkClient 对象
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {

            // 收到事件通知后的回调函数（用户的业务逻辑）
            System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());

            // 再次启动监听
            try {
                zkClient.getChildren("/",true);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    /** 创建子节点 */
    @Test
    public void createNode() throws Exception{
        /*
         * 参数1：要创建的节点的路径
         * 参数2：节点数据
         * 参数3：节点权限
         * 参数4：节点的类型（persistent持久 or ephemeral临时）
         * */
        String nodeCreated = zkClient.create(
                "/he-yi-zk-test",
                "zk-test".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        System.out.println(nodeCreated);
    }

    /** 获取子节点并监听变化 */
    @Test
    public void getChildren() throws Exception{
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    /**  判断 Zk node 是否存在*/
    @Test
    public void exist() throws Exception{
        Stat stat = zkClient.exists("/he-yi-zk-test", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }

}
