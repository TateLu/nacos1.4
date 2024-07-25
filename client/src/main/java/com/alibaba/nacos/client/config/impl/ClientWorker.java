/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;
import static com.alibaba.nacos.api.common.Constants.ENCRYPTED_DATA_KEY;
import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {
    
    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);
    
    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }
    
    /**
     * Add listeners for tenant with content.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param content   content
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content,
            List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        cache.setContent(content);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }
    
    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group, tenant);
            }
        }
    }
    
    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);
        
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }
    
    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }
        
        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            //fix issue # 1317
            if (enableRemoteSyncConfig) {
                ConfigResponse response = getServerConfig(dataId, group, tenant, 3000L);
                cacheData.setContent(response.getContent());
            }
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            cacheData.setTaskId(taskId);
            lastCacheData = cacheData;
        }
        
        // reset so that server not hang this check
        lastCacheData.setInitializing(true);
        
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
        
        return lastCacheData;
    }
    
    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
    }
    
    public ConfigResponse getServerConfig(String dataId, String group, String tenant, long readTimeout)
            throws NacosException {
        ConfigResponse configResponse = new ConfigResponse();
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }
        
        HttpRestResult<String> result = null;
        try {
            Map<String, String> params = new HashMap<String, String>(3);
            if (StringUtils.isBlank(tenant)) {
                params.put("dataId", dataId);
                params.put("group", group);
            } else {
                params.put("dataId", dataId);
                params.put("group", group);
                params.put("tenant", tenant);
            }
            result = agent.httpGet(Constants.CONFIG_CONTROLLER_PATH, null, params, agent.getEncode(), readTimeout);
        } catch (Exception ex) {
            String message = String
                    .format("[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s",
                            agent.getName(), dataId, group, tenant);
            LOGGER.error(message, ex);
            throw new NacosException(NacosException.SERVER_ERROR, ex);
        }
        
        switch (result.getCode()) {
            case HttpURLConnection.HTTP_OK:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, result.getData());
                configResponse.setContent(result.getData());
                String configType;
                if (result.getHeader().getValue(CONFIG_TYPE) != null) {
                    configType = result.getHeader().getValue(CONFIG_TYPE);
                } else {
                    configType = ConfigType.TEXT.getType();
                }
                configResponse.setConfigType(configType);
                String encryptedDataKey = result.getHeader().getValue(ENCRYPTED_DATA_KEY);
                LocalEncryptedDataKeyProcessor
                        .saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, encryptedDataKey);
                configResponse.setEncryptedDataKey(encryptedDataKey);
                return configResponse;
            case HttpURLConnection.HTTP_NOT_FOUND:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, null);
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, null);
                return configResponse;
            case HttpURLConnection.HTTP_CONFLICT: {
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
            case HttpURLConnection.HTTP_FORBIDDEN: {
                LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}", agent.getName(),
                        dataId, group, tenant);
                throw new NacosException(result.getCode(), result.getMessage());
            }
            default: {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", agent.getName(),
                        dataId, group, tenant, result.getCode());
                throw new NacosException(result.getCode(),
                        "http error, code=" + result.getCode() + ",dataId=" + dataId + ",group=" + group + ",tenant="
                                + tenant);
            }
        }
    }
    //failover:a procedure by which a system automatically transfers control to a duplicate system when it detects a fault or failure.
    //书签 配置中心 客户端 failover配置文件
    private void checkLocalConfig(CacheData cacheData) {
        final String dataId = cacheData.dataId;
        final String group = cacheData.group;
        final String tenant = cacheData.tenant;
        File path = LocalConfigInfoProcessor.getFailoverFile(agent.getName(), dataId, group, tenant);
        //当isUseLocalConfigInfo=false 且 failover配置文件存在时，使用failover配置文件，并更新内存中的配置
        if (!cacheData.isUseLocalConfigInfo() && path.exists()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            // FIXME temporary fix https://github.com/alibaba/nacos/issues/7039
            String encryptedDataKey = LocalEncryptedDataKeyProcessor
                    .getEncryptDataKeyFailover(agent.getName(), dataId, group, tenant);
            cacheData.setEncryptedDataKey(encryptedDataKey);
            cacheData.setContent(content);
            
            LOGGER.warn(
                    "[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            return;
        }
        
        // If use local config info, then it doesn't notify business listener and notify after getting from server.
        if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
            cacheData.setUseLocalConfigInfo(false);
            LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", agent.getName(),
                    dataId, group, tenant);
            return;
        }
        
        // When it changed.
        if (cacheData.isUseLocalConfigInfo() && path.exists() && cacheData.getLocalConfigInfoVersion() != path
                .lastModified()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            // FIXME temporary fix https://github.com/alibaba/nacos/issues/7039
            String encryptedDataKey = LocalEncryptedDataKeyProcessor
                    .getEncryptDataKeyFailover(agent.getName(), dataId, group, tenant);
            cacheData.setEncryptedDataKey(encryptedDataKey);
            cacheData.setContent(content);
            LOGGER.warn(
                    "[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
        }
    }
    
    private String blank2defaultGroup(String group) {
        return StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
    }
    
    /**
     * Check config info.
     */
    //书签 配置中心 客户端 提交长轮询任务
    public void checkConfigInfo() {
        // Dispatch tasks.
        int listenerSize = cacheMap.size();
        // Round up the longingTaskCount.
        int longingTaskCount = (int) Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize());
        if (longingTaskCount > currentLongingTaskCount) {
            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                executorService.execute(new LongPollingRunnable(i));
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }
    
    /**
     * Fetch the dataId list from server.
     *
     * @param cacheDatas              CacheDatas for config information.
     * @param inInitializingCacheList initial cache lists.
     * @return String include dataId and group (ps: it maybe null).
     * @throws Exception Exception.
     */
    List<String> checkUpdateDataIds(List<CacheData> cacheDatas, List<String> inInitializingCacheList) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (CacheData cacheData : cacheDatas) {
            if (!cacheData.isUseLocalConfigInfo()) {
                sb.append(cacheData.dataId).append(WORD_SEPARATOR);
                sb.append(cacheData.group).append(WORD_SEPARATOR);
                if (StringUtils.isBlank(cacheData.tenant)) {
                    sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
                } else {
                    sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
                    sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
                }
                if (cacheData.isInitializing()) {
                    // It updates when cacheData occurs in cacheMap by first time.
                    inInitializingCacheList
                            .add(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant));
                }
            }
        }
        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        return checkUpdateConfigStr(sb.toString(), isInitializingCacheList);
    }
    
    /**
     * Fetch the updated dataId list from server.
     *
     * @param probeUpdateString       updated attribute string value.
     * @param isInitializingCacheList initial cache lists.
     * @return The updated dataId list(ps: it maybe null).
     * @throws IOException Exception.
     */
    List<String> checkUpdateConfigStr(String probeUpdateString, boolean isInitializingCacheList) throws Exception {

        Map<String, String> params = new HashMap<String, String>(2);
        params.put(Constants.PROBE_MODIFY_REQUEST, probeUpdateString);
        Map<String, String> headers = new HashMap<String, String>(2);
        headers.put("Long-Pulling-Timeout", "" + timeout);
        
        // told server do not hang me up if new initializing cacheData added in
        if (isInitializingCacheList) {
            headers.put("Long-Pulling-Timeout-No-Hangup", "true");
        }
        
        if (StringUtils.isBlank(probeUpdateString)) {
            return Collections.emptyList();
        }
        
        try {
            // In order to prevent the server from handling the delay of the client's long task,
            // increase the client's read timeout to avoid this problem.

            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1);
            HttpRestResult<String> result = agent
                    .httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener", headers, params, agent.getEncode(),
                            readTimeoutMs);
            
            if (result.ok()) {
                setHealthServer(true);
                return parseUpdateDataIdResponse(result.getData());
            } else {
                setHealthServer(false);
                LOGGER.error("[{}] [check-update] get changed dataId error, code: {}", agent.getName(),
                        result.getCode());
            }
        } catch (Exception e) {
            setHealthServer(false);
            LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception", e);
            throw e;
        }
        return Collections.emptyList();
    }
    
    /**
     * Get the groupKey list from the http response.
     *
     * @param response Http response.
     * @return GroupKey List, (ps: it maybe null).
     */
    private List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isBlank(response)) {
            return Collections.emptyList();
        }
        
        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error", e);
        }
        
        List<String> updateList = new LinkedList<String>();
        
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isBlank(dataIdAndGroup)) {
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 2) {
                    updateList.add(GroupKey.getKey(dataId, group));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}", agent.getName(), dataId,
                            group);
                } else if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}", agent.getName(),
                            dataId, group, tenant);
                } else {
                    LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}", agent.getName(),
                            dataIdAndGroup);
                }
            }
        }
        return updateList;
    }

    //书签 配置中心 客户端 ClientWorker启动（启动后台线程池，获取服务端信息）
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    /**
     * ClientWorker的构造函数，负责初始化与配置相关的各种组件和线程池。
     *
     * @param agent HttpAgent实例，用于HTTP请求。
     * @param configFilterChainManager 配置过滤器链管理器，用于处理配置的过滤和转换。
     * @param properties 配置属性，用于初始化和配置ClientWorker的行为。
     */
    public ClientWorker(final HttpAgent agent, final ConfigFilterChainManager configFilterChainManager,
            final Properties properties) {
        this.agent = agent;
        this.configFilterChainManager = configFilterChainManager;

        // 初始化配置
        init(properties);

        // 书签 配置中心 客户端 ClientWorker 线程池使用

        // 创建一个定时线程池，用于执行周期性任务。
        this.executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                // 设置线程名称，方便识别和调试
                t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
                t.setDaemon(true); // 设置为守护线程，确保程序退出时这些线程不会阻止进程退出。
                return t;
            }
        });

        // 创建一个定长线程池，用于处理长轮询任务。
        this.executorService = Executors
                .newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        // 设置线程名称，方便识别和调试
                        t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
                        t.setDaemon(true); // 设置为守护线程
                        return t;
                    }
                });

        // 定时任务，周期性检查配置信息。
        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    checkConfigInfo(); // 检查并处理配置信息的更新。
                } catch (Throwable e) {
                    LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error", e);
                }
            }
        }, 1L, 10L, TimeUnit.MILLISECONDS);
    }

    
    private void init(Properties properties) {
        
        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
                Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);
        
        taskPenaltyTime = ConvertUtils
                .toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME), Constants.CONFIG_RETRY_TIME);
        
        this.enableRemoteSyncConfig = Boolean
                .parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, LOGGER);
        ThreadUtils.shutdownThreadPool(executor, LOGGER);
        LOGGER.info("{} do shutdown stop", className);
    }
    /**
     * 处理failover配置：判断当前CacheData是否使用failover配置（ClientWorker.checkLocalConfig），如果使用failover配置，则校验本地配置文件内容是否发生变化，发生变化则触发监听器（CacheData.checkListenerMd5）。这一步其实和长轮询无关。
     * 对于所有非failover配置，执行长轮询，返回发生改变的groupKey（ClientWorker.checkUpdateDataIds）。
     * 根据返回的groupKey，查询服务端实时配置并保存snapshot（ClientWorker.getServerConfig）
     * 更新内存CacheData的配置content。
     * 校验配置是否发生变更，通知监听器（CacheData.checkListenerMd5）。
     * 如果正常执行本次长轮询，立即提交长轮询任务，执行下一次长轮询；发生异常，延迟2s提交长轮询任务。
     *
     * */
    //书签 配置中心 客户端 长轮询线程
    class LongPollingRunnable implements Runnable {
        
        private final int taskId;
        
        public LongPollingRunnable(int taskId) {
            this.taskId = taskId;
        }
        
        @Override
        /**
         * 实现Runnable接口的run方法，用于定期从配置中心获取更新的配置。
         * 此方法主要负责以下步骤：
         * 1. 根据当前任务ID，从缓存中筛选出相关的配置数据。
         * 2. 检查并更新这些配置数据的本地配置信息。
         * 3. 对于有更新的配置数据，从配置中心获取最新的配置内容并更新缓存。
         * 4. 触发配置变更监听器。
         * 5. 如果在执行过程中发生异常，将重新调度任务执行。
         */
        public void run() {
            // 初始化用于存储匹配任务ID的配置数据的列表和正在初始化的配置列表
            List<CacheData> cacheDatas = new ArrayList<CacheData>();
            List<String> inInitializingCacheList = new ArrayList<String>();

            try {
                // 遍历缓存中的所有配置数据，筛选出与当前任务ID匹配的配置数据
                for (CacheData cacheData : cacheMap.values()) {
                    if (cacheData.getTaskId() == taskId) {
                        cacheDatas.add(cacheData);
                        try {
                            // 检查并更新配置的本地信息
                            checkLocalConfig(cacheData);
                            if (cacheData.isUseLocalConfigInfo()) {
                                cacheData.checkListenerMd5();
                            }
                        } catch (Exception e) {
                            // 记录获取本地配置信息时的错误
                            LOGGER.error("get local config info error", e);
                        }
                    }
                }

                // 发起长轮询请求，监听更新的配置
                List<String> changedGroupKeys = checkUpdateDataIds(cacheDatas, inInitializingCacheList);
                if (!CollectionUtils.isEmpty(changedGroupKeys)) {
                    LOGGER.info("get changedGroupKeys:" + changedGroupKeys);
                }

                // 遍历有更新的配置数据，从配置中心获取最新配置并更新缓存
                for (String groupKey : changedGroupKeys) {
                    // 解析组键获取配置的ID、组和租户信息
                    String[] key = GroupKey.parseKey(groupKey);
                    String dataId = key[0];
                    String group = key[1];
                    String tenant = null;
                    if (key.length == 3) {
                        tenant = key[2];
                    }
                    try {
                        // 从配置中心获取最新配置 /v1/cs/configs
                        ConfigResponse response = getServerConfig(dataId, group, tenant, 3000L);
                        // 更新缓存中的配置数据
                        CacheData cache = cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
                        cache.setEncryptedDataKey(response.getEncryptedDataKey());
                        cache.setContent(response.getContent());
                        if (null != response.getConfigType()) {
                            cache.setType(response.getConfigType());
                        }
                        // 记录配置更新日志
                        LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                                agent.getName(), dataId, group, tenant, cache.getMd5(),
                                ContentUtils.truncateContent(response.getContent()), response.getConfigType());
                    } catch (NacosException ioe) {
                        // 记录获取配置时的异常
                        String message = String
                                .format("[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s",
                                        agent.getName(), dataId, group, tenant);
                        LOGGER.error(message, ioe);
                    }
                }

                // 检查并触发配置变更监听器
                for (CacheData cacheData : cacheDatas) {
                    if (!cacheData.isInitializing() || inInitializingCacheList
                            .contains(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant))) {
                        cacheData.checkListenerMd5();
                        cacheData.setInitializing(false);
                    }
                }
                // 清空正在初始化的配置列表，准备下一轮循环
                inInitializingCacheList.clear();
                // 重新提交当前任务到执行器服务，实现周期性执行
                executorService.execute(this);

            } catch (Throwable e) {
                // 记录长轮询过程中的错误，并根据惩罚时间重新调度任务
                //报错，则延迟一定时间再提交
                // If the rotation training task is abnormal, the next execution time of the task will be punished
                LOGGER.error("longPolling error : ", e);
                executorService.schedule(this, taskPenaltyTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    
    public boolean isHealthServer() {
        return isHealthServer;
    }
    
    private void setHealthServer(boolean isHealthServer) {
        this.isHealthServer = isHealthServer;
    }
    
    final ScheduledExecutorService executor;
    
    final ScheduledExecutorService executorService;
    
    /**
     * groupKey -> cacheData.
     */
    private final ConcurrentHashMap<String, CacheData> cacheMap = new ConcurrentHashMap<String, CacheData>();
    
    private final HttpAgent agent;
    
    private final ConfigFilterChainManager configFilterChainManager;
    
    private boolean isHealthServer = true;
    
    private long timeout;
    
    private double currentLongingTaskCount = 0;
    
    private int taskPenaltyTime;
    
    private boolean enableRemoteSyncConfig = false;
}
