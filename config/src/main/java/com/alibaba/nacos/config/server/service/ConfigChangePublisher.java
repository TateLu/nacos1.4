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

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.config.server.service.sql.EmbeddedStorageContextUtils;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.sys.env.EnvUtil;

import java.sql.Timestamp;

/**
 * ConfigChangePublisher.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class ConfigChangePublisher {
    
    /**
     * Notify ConfigChange.
     *
     * @param event ConfigDataChangeEvent instance.
     */
    public static void notifyConfigChange(ConfigDataChangeEvent event) {

        if (PropertyUtil.isEmbeddedStorage() && !EnvUtil.getStandaloneMode()) {
            /**
             * 1 如果使用derby数据源 2 使用集群模式，将直接返回
             * 2 使用raft协议通知集群节点 {@link EmbeddedStorageContextUtils#onModifyConfigTagInfo(ConfigInfo, String, String, Timestamp)}
             * */
            return;
        }
        /**
         * 其他情况，比如使用外部数据源（mysql）
         *
         * 1 使用http请求通知其他集群节点
         * 2 发布事件通知长轮询客户端
         * */
        NotifyCenter.publishEvent(event);
    }
    
}
