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

package com.alibaba.nacos.config.server.service.sql;

import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.event.ConfigDumpEvent;
import com.alibaba.nacos.sys.env.EnvUtil;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Temporarily saves all insert, update, and delete statements under a transaction in the order in which they occur.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class EmbeddedStorageContextUtils {
    
    private static final ThreadLocal<ArrayList<ModifyRequest>> SQL_CONTEXT = ThreadLocal.withInitial(ArrayList::new);
    
    private static final ThreadLocal<Map<String, String>> EXTEND_INFO_CONTEXT = ThreadLocal.withInitial(HashMap::new);
    
    /**
     * Add sql context.
     *
     * @param sql  sql
     * @param args argument list
     */
    public static void addSqlContext(String sql, Object... args) {
        ArrayList<ModifyRequest> requests = SQL_CONTEXT.get();
        ModifyRequest context = new ModifyRequest();
        context.setExecuteNo(requests.size());
        context.setSql(sql);
        context.setArgs(args);
        requests.add(context);
        SQL_CONTEXT.set(requests);
    }
    
    /**
     * Put extend info.
     *
     * @param key   key
     * @param value value
     */
    public static void putExtendInfo(String key, String value) {
        Map<String, String> old = EXTEND_INFO_CONTEXT.get();
        old.put(key, value);
        EXTEND_INFO_CONTEXT.set(old);
    }
    
    /**
     * Put all extend info.
     *
     * @param map all extend info
     */
    public static void putAllExtendInfo(Map<String, String> map) {
        Map<String, String> old = EXTEND_INFO_CONTEXT.get();
        old.putAll(map);
        EXTEND_INFO_CONTEXT.set(old);
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     *
     *
     * 当配置信息被修改时，触发配置信息的dump操作。
     *
     * 集群模式下，通过raft状态机异步实现水平通知，并携带相关信息。
     *
     * @param configInfo 配置信息对象，包含配置的租户、数据ID、分组、内容等信息。
     * @param srcIp 发起配置修改的源IP地址，用于追踪和审计。
     * @param time 配置修改的时间戳，精确到毫秒，用于记录操作时间。
     */
    public static void onModifyConfigInfo(ConfigInfo configInfo, String srcIp, Timestamp time) {
        // 检查是否为非独立部署模式，非独立部署模式下才进行配置dump操作
        if (!EnvUtil.getStandaloneMode()) {
            // 构建配置dump事件对象，包含配置的详细信息及操作源IP和时间
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(false).namespaceId(configInfo.getTenant())
                    .dataId(configInfo.getDataId()).group(configInfo.getGroup()).isBeta(false)
                    .content(configInfo.getContent()).type(configInfo.getType()).handleIp(srcIp)
                    .lastModifiedTs(time.getTime()).build();

            // 准备扩展信息映射，用于存储配置dump事件的JSON字符串
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));

            // 将配置dump事件的扩展信息存储到嵌入式存储上下文中
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }

    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param configInfo {@link ConfigInfo}
     * @param betaIps    Receive client IP for grayscale configuration publishing
     * @param srcIp      The IP of the operator
     * @param time       Operating time
     */
    public static void onModifyConfigBetaInfo(ConfigInfo configInfo, String betaIps, String srcIp, Timestamp time) {
        if (!EnvUtil.getStandaloneMode()) {
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(false).namespaceId(configInfo.getTenant())
                    .dataId(configInfo.getDataId()).group(configInfo.getGroup()).isBeta(true).betaIps(betaIps)
                    .content(configInfo.getContent()).type(configInfo.getType()).handleIp(srcIp)
                    .lastModifiedTs(time.getTime()).build();
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param configInfo {@link ConfigInfo}
     * @param tag        tag info
     * @param srcIp      The IP of the operator
     * @param time       Operating time
     */
    public static void onModifyConfigTagInfo(ConfigInfo configInfo, String tag, String srcIp, Timestamp time) {
        if (!EnvUtil.getStandaloneMode()) {
            //raft协议 来通知其他节点更新
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(false).namespaceId(configInfo.getTenant())
                    .dataId(configInfo.getDataId()).group(configInfo.getGroup()).isBeta(false).tag(tag)
                    .content(configInfo.getContent()).type(configInfo.getType()).handleIp(srcIp)
                    .lastModifiedTs(time.getTime()).build();
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param namespaceId namespaceId
     * @param group       groupName
     * @param dataId      dataId
     * @param srcIp       The IP of the operator
     * @param time        Operating time
     */
    public static void onDeleteConfigInfo(String namespaceId, String group, String dataId, String srcIp,
            Timestamp time) {
        if (!EnvUtil.getStandaloneMode()) {
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(true).namespaceId(namespaceId).group(group)
                    .dataId(dataId).isBeta(false).handleIp(srcIp).lastModifiedTs(time.getTime()).build();
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param configInfos {@link ConfigInfo} list
     */
    public static void onBatchDeleteConfigInfo(List<ConfigInfo> configInfos) {
        if (!EnvUtil.getStandaloneMode()) {
            List<ConfigDumpEvent> events = new ArrayList<>();
            for (ConfigInfo configInfo : configInfos) {
                String namespaceId =
                        StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
                ConfigDumpEvent event = ConfigDumpEvent.builder().remove(true).namespaceId(namespaceId)
                        .group(configInfo.getGroup()).dataId(configInfo.getDataId()).isBeta(false).build();
                
                events.add(event);
            }
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFOS_CONFIG_DUMP_EVENT, JacksonUtils.toJson(events));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param namespaceId namespaceId
     * @param group       group
     * @param dataId      dataId
     * @param time        Operating time
     */
    public static void onDeleteConfigBetaInfo(String namespaceId, String group, String dataId, long time) {
        if (!EnvUtil.getStandaloneMode()) {
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(true).namespaceId(namespaceId).dataId(dataId)
                    .group(group).isBeta(true).build();
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * In the case of the in-cluster storage mode, the logic of horizontal notification is implemented asynchronously
     * via the raft state machine, along with the information.
     *
     * @param namespaceId namespaceId
     * @param group       group
     * @param dataId      dataId
     * @param tag         tag info
     * @param srcIp       The IP of the operator
     */
    public static void onDeleteConfigTagInfo(String namespaceId, String group, String dataId, String tag,
            String srcIp) {
        if (!EnvUtil.getStandaloneMode()) {
            ConfigDumpEvent event = ConfigDumpEvent.builder().remove(true).namespaceId(namespaceId).group(group)
                    .dataId(dataId).isBeta(true).tag(tag).handleIp(srcIp).build();
            
            Map<String, String> extendInfo = new HashMap<>(2);
            extendInfo.put(Constants.EXTEND_INFO_CONFIG_DUMP_EVENT, JacksonUtils.toJson(event));
            EmbeddedStorageContextUtils.putAllExtendInfo(extendInfo);
        }
    }
    
    /**
     * Determine if key is included.
     *
     * @param key key
     * @return {@code true} if contains key
     */
    public static boolean containsExtendInfo(String key) {
        Map<String, String> extendInfo = EXTEND_INFO_CONTEXT.get();
        boolean exist = extendInfo.containsKey(key);
        EXTEND_INFO_CONTEXT.set(extendInfo);
        return exist;
    }
    
    public static List<ModifyRequest> getCurrentSqlContext() {
        return SQL_CONTEXT.get();
    }
    
    public static Map<String, String> getCurrentExtendInfo() {
        return EXTEND_INFO_CONTEXT.get();
    }
    
    public static void cleanAllContext() {
        SQL_CONTEXT.remove();
        EXTEND_INFO_CONTEXT.remove();
    }
    
}
