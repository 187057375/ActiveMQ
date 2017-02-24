package com.sdu.activemq.core.zk;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public class ZkClientContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientContext.class);

    private ZkConfig config;

    private CuratorFramework framework;

    private AtomicBoolean start = new AtomicBoolean(false);

    // 节点监听
    private List<PathChildrenCache> watchers;

    public ZkClientContext(ZkConfig config) {
        this.config = config;
    }

    public void start() throws InterruptedException {
        framework = CuratorFrameworkFactory.builder()
                                           .connectString(config.getZkServer())
                                           .connectionTimeoutMs(config.getZkServerConnectTimeout())
                                           .retryPolicy(new RetryNTimes(config.getZkRetryTimes(), config.getZkRetrySleepInterval()))
                                           .build();

        // 启动
        framework.start();

        CuratorFrameworkState state = framework.getState();
        framework.blockUntilConnected();
        if (state == CuratorFrameworkState.STARTED) {
            start.set(true);
            LOGGER.info("connect zk server[{}] success", config.getZkServer());
        }

        watchers = Lists.newLinkedList();
    }

    public boolean isServing() {
        return start.get();
    }

    public void destroy() throws Exception {
        start.set(false);
        if (framework != null) {
            framework.close();
            framework = null;
        }

        if (watchers != null) {
            for (PathChildrenCache watcher : watchers) {
                watcher.close();
            }
        }
    }

    public void addPathListener(String path, boolean cacheData, PathChildrenCacheListener listener) throws Exception {
        valid();
        PathChildrenCache watcher = new PathChildrenCache(framework, path, cacheData);
        watcher.getListenable().addListener(listener);
        watcher.start();
        watchers.add(watcher);
    }

    public void updateNodeData(String path, byte[] data) throws Exception {
        valid();
        framework.setData().forPath(path, data);
    }

    public byte[] getNodeData(String path) throws Exception {
        valid();
        return framework.getData().forPath(path);
    }

    public boolean isNodeExist(String path) throws Exception {
        valid();
        Stat stat = framework.checkExists().forPath(path);
        return stat != null;
    }

    public List<String> getChildNode(String parentPath) throws Exception {
        valid();
        return framework.getChildren().forPath(parentPath);
    }

    public String createNode(String path, byte []data) throws Exception {
        valid();
        return framework.create().creatingParentsIfNeeded().forPath(path, data);
    }

    public void deleteNode(String path) throws Exception {
        valid();
        Stat stat = framework.checkExists().forPath(path);
        if (stat != null) {
            framework.delete().forPath(path);
        }
    }

    private void valid() {
        if (!start.get()) {
            throw new IllegalStateException("already closed zk connect");
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        ZkConfig zkConfig = new ZkConfig(new MQConfig(props));
        ZkClientContext zkClientContext = new ZkClientContext(zkConfig);
        zkClientContext.start();

        List<String> child = zkClientContext.getChildNode("/test");
        System.out.println("child : " + child);

        zkClientContext.deleteNode("/test/1");

        child = zkClientContext.getChildNode("/");
        System.out.println("after delete child : " + child);

        zkClientContext.addPathListener("/test", true, new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData childData = event.getData();
                if (childData == null) {
                    return;
                }
                String path = childData.getPath();
                byte[] data = childData.getData();
                Stat stat = childData.getStat();
                System.out.println("path : " + path);
                System.out.println("data : " + new String(data));
                System.out.println(stat);
            }
        });
        zkClientContext.createNode("/test/1", "test".getBytes());

    }

}
