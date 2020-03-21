package com.hadoop.yi.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

/**
 * Description:  java 客户端对zk的节点进行操作
 * Version:1.0
 * Author: an.phy
 * <p> file created at 2019/7/8 </p>
 */
public class NodeTest {

    private CuratorFramework client;

    /**
     * 初始化client对象
     */
    @Before
    public void initClient() {
        // zk服务地址串
        String connectString = "192.168.12.151:2181" +
                ",192.168.12.152:2181" +
                ",192.168.12.153:2181";
        // 创建重试策略对象
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        // 创建客户端对象
        client = CuratorFrameworkFactory.newClient(connectString
                , 1000, 1000
                , retryPolicy);
    }

    /**
     * 1. 创建永久节点
     */
    @Test
    public void createPermanentNode() throws Exception {
        // 开启客户端
        client.start();
        // 通过create创建永久节点
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/hello/world");
        // 关闭客户端
        client.close();
    }

    /**
     * 2. 创建临时节点
     */
    @Test
    public void createEphemeralNode() throws Exception {
        // 开启客户端
        client.start();
        // 创建临时节点
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/hello1/world");
        Thread.sleep(5000);
        client.close();
    }

    /**
     * 3. 修改节点数据
     */
    @Test
    public void updateNodeData() throws Exception {
        // 开启客户端
        client.start();
        // 修改节点数据
        client.setData().forPath("/hello1", "world1".getBytes());
        // 关闭客户端
        client.close();
    }

    /**
     * 4. 节点查询
     */
    @Test
    public void queryNode() throws Exception {
        // 开启客户端
        client.start();
        byte[] helloData = client.getData().forPath("/hello");
        System.out.println(new String(helloData));
        // 关闭客户端
        client.close();
    }

    /**
     * 5. zk watch 机制
     */
    @Test
    public void watchNode() throws Exception {
        // 开启 zk 客户端
        client.start();
        // 设置节点缓存
        TreeCache treeCache = new TreeCache(client, "/hello2");
        // 设置监听器和处理过程
        treeCache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
            ChildData data = treeCacheEvent.getData();
            if (data != null) {
                switch (treeCacheEvent.getType()) {
                    case NODE_ADDED:
                        System.out.println("NODE_ADDED ："
                                + data.getPath() + " 数据"
                                + new String(data.getData()));
                        break;
                    case NODE_REMOVED:
                        System.out.println("NODE_REMOVED ："
                                + data.getPath() + " 数据"
                                + new String(data.getData()));
                        break;
                    case NODE_UPDATED:
                        System.out.println("NODE_UPDATED ："
                                + data.getPath() + " 数据"
                                + new String(data.getData()));
                        break;
                    default:
                        break;
                }
            } else {
                System.out.println("data is null " + treeCacheEvent.getType());
            }
        });
        treeCache.start();
        Thread.sleep(3000);
    }
}
