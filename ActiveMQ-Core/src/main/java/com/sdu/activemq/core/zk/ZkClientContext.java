package com.sdu.activemq.core.zk;

import com.google.common.collect.Lists;
import com.sdu.activemq.core.MQConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sdu.activemq.utils.Const.ZK_MQ_LOCK_PATH;
import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.POST_INITIALIZED_EVENT;

/**
 * @author hanhan.zhang
 * */
public class ZkClientContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientContext.class);

    private ZkConfig config;

    private CuratorFramework framework;

    private AtomicBoolean start = new AtomicBoolean(false);

    // 分布式锁
    private InterProcessMutex processMutex;

    // 节点监听
    private List<PathChildrenCache> watchers;

    // 所有子节点监听
    private List<TreeCache> treeWatchers;

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

        processMutex = new InterProcessMutex(framework, ZK_MQ_LOCK_PATH);

        CuratorFrameworkState state = framework.getState();
        framework.blockUntilConnected();
        if (state == CuratorFrameworkState.STARTED) {
            start.set(true);
            LOGGER.info("connect zk server[{}] success", config.getZkServer());
        }

        watchers = Lists.newLinkedList();
        treeWatchers = Lists.newLinkedList();
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

        if (treeWatchers != null) {
            for (TreeCache watcher : treeWatchers) {
                watcher.close();
            }
        }
    }

    // 仅对子节点监控
    public void addPathListener(String path, boolean cacheData, PathChildrenCacheListener listener) throws Exception {
        valid();
        PathChildrenCache watcher = new PathChildrenCache(framework, path, cacheData);
        watcher.getListenable().addListener(listener);
        watcher.start(POST_INITIALIZED_EVENT);
        watchers.add(watcher);
    }

    public void addSubAllPathListener(String path, TreeCacheListener listener) throws Exception {
        valid();
        TreeCache treeCache = new TreeCache(framework, path);
        treeCache.getListenable().addListener(listener);
        treeCache.start();
        treeWatchers.add(treeCache);
    }

    public void updateNodeData(String path, byte[] data) {
        valid();
        try {
            framework.setData().forPath(path, data);
        } catch (Exception e) {
            LOGGER.error("Update zk node[{}] data exception", path, e);
        }

    }

    public byte[] getNodeData(String path) throws Exception {
        valid();
        Stat stat = new Stat();
        return framework.getData().storingStatIn(stat).forPath(path);
    }

    public boolean isNodeExist(String path) throws Exception {
        valid();
        Stat stat = framework.checkExists().forPath(path);
        return stat != null;
    }

    public List<String> getChildNode(String parentPath) {
        valid();
        try {
            return framework.getChildren().forPath(parentPath);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public String createNode(String path, String data) throws Exception {
       return createNode(path, data.getBytes());
    }

    public String createNode(String path, byte []data) throws Exception {
        valid();
        return framework.create().creatingParentsIfNeeded().forPath(path, data);
    }

    public void deleteNode(String path) throws Exception {
        valid();
        Stat stat = framework.checkExists().forPath(path);
        if (stat != null) {
            framework.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
        }
    }

    public InterProcessMutex getProcessMutex() {
        return processMutex;
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

        zkClientContext.addSubAllPathListener("/test", new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("type : " + event.getType());
                ChildData data = event.getData();
                System.out.println("path : " + data.getPath());
                System.out.println("data : " + new String(data.getData()));
            }
        });

        for (int i = 0; i < 3; ++i) {
            zkClientContext.updateNodeData("/test/1/2", UUID.randomUUID().toString().getBytes());
        }

    }

}
