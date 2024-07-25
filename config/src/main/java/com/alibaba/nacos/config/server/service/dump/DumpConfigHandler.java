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

package com.alibaba.nacos.config.server.service.dump;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.config.server.model.event.ConfigDumpEvent;
import com.alibaba.nacos.config.server.service.AggrWhitelist;
import com.alibaba.nacos.config.server.service.ClientIpWhiteList;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.SwitchService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;

/**
 * Dump config subscriber.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class DumpConfigHandler extends Subscriber<ConfigDumpEvent> {
    
    /**
     * trigger config dump event.
     *
     * @param event {@link ConfigDumpEvent}
     * @return {@code true} if the config dump task success , else {@code false}
     */
    /**
     * 处理配置dump事件。
     * 根据事件的特性（beta、标签、删除标志等），将配置信息存储到缓存中，
     * 并记录相应的操作日志。
     *
     * @param event 配置dump事件，包含配置的相关信息如数据ID、分组、内容等。
     * @return 操作是否成功的布尔值。
     */
    public static boolean configDump(ConfigDumpEvent event) {
        // 获取事件中的配置信息
        final String dataId = event.getDataId();
        final String group = event.getGroup();
        final String namespaceId = event.getNamespaceId();
        final String content = event.getContent();
        final String type = event.getType();
        final long lastModified = event.getLastModifiedTs();

        // 处理beta配置
        if (event.isBeta()) {
            boolean result = false;
            // 如果是移除beta配置
            if (event.isRemove()) {
                result = ConfigCacheService.removeBeta(dataId, group, namespaceId);
                if (result) {
                    // 记录移除成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                }
                return result;
            } else {
                // 添加或更新beta配置
                result = ConfigCacheService.dumpBeta(dataId, group, namespaceId, content, lastModified, event.getBetaIps());
                if (result) {
                    // 记录添加或更新成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified, content.length());
                }
                return result;
            }
        }

        // 处理非beta配置，且无标签的情况
        if (StringUtils.isBlank(event.getTag())) {
            // 特殊配置处理，如聚合白名单、客户端IP白名单、开关服务配置
            if (dataId.equals(AggrWhitelist.AGGRIDS_METADATA)) {
                AggrWhitelist.load(content);
            }
            if (dataId.equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
                ClientIpWhiteList.load(content);
            }
            if (dataId.equals(SwitchService.SWITCH_META_DATAID)) {
                SwitchService.load(content);
            }

            boolean result;
            // 配置添加或更新
            if (!event.isRemove()) {
                result = ConfigCacheService.dump(dataId, group, namespaceId, content, lastModified, type);
                if (result) {
                    // 记录添加或更新成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified, content.length());
                }
            } else {
                // 配置移除
                result = ConfigCacheService.remove(dataId, group, namespaceId);
                if (result) {
                    // 记录移除成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                }
            }
            return result;
        } else {
            // 处理带标签的配置
            boolean result;
            // 配置添加或更新
            if (!event.isRemove()) {
                result = ConfigCacheService.dumpTag(dataId, group, namespaceId, event.getTag(), content, lastModified);
                if (result) {
                    // 记录添加或更新成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified, content.length());
                }
            } else {
                // 配置移除
                result = ConfigCacheService.removeTag(dataId, group, namespaceId, event.getTag());
                if (result) {
                    // 记录移除成功的日志
                    ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                            ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                }
            }
            return result;
        }
    }
    
    @Override
    public void onEvent(ConfigDumpEvent event) {
        configDump(event);
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ConfigDumpEvent.class;
    }
}
