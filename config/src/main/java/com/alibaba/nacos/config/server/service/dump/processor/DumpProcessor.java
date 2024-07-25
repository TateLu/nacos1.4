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

package com.alibaba.nacos.config.server.service.dump.processor;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo4Beta;
import com.alibaba.nacos.config.server.model.ConfigInfo4Tag;
import com.alibaba.nacos.config.server.model.event.ConfigDumpEvent;
import com.alibaba.nacos.config.server.service.dump.DumpConfigHandler;
import com.alibaba.nacos.config.server.service.dump.DumpService;
import com.alibaba.nacos.config.server.service.dump.task.DumpTask;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * dump processor.
 *
 * @author Nacos
 * @date 2020/7/5 12:19 PM
 */
public class DumpProcessor implements NacosTaskProcessor {
    
    public DumpProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
    }

    //书签 配置中心 服务端 配置写入数据库、文件系统
    @Override
    /**
     * 处理Nacos任务，根据任务类型执行相应的配置dump操作。
     *
     * @param task 需要处理的Nacos任务，此任务封装了配置的相关信息。
     * @return 是否成功执行了配置dump操作。
     */
    public boolean process(NacosTask task) {
        // 配置写入数据库或文件系统
        final PersistService persistService = dumpService.getPersistService();

        DumpTask dumpTask = (DumpTask) task;

        // 从任务中解析出配置的dataId、group和tenant信息。
        String[] pair = GroupKey2.parseKey(dumpTask.getGroupKey());
        String dataId = pair[0];
        String group = pair[1];
        String tenant = pair[2];
        // 获取配置的最后修改时间、处理IP、是否是Beta发布、标签等信息。
        long lastModified = dumpTask.getLastModified();
        String handleIp = dumpTask.getHandleIp();
        boolean isBeta = dumpTask.isBeta();
        String tag = dumpTask.getTag();

        // 根据配置信息构建ConfigDumpEvent对象，用于后续的配置dump操作。
        ConfigDumpEvent.ConfigDumpEventBuilder build = ConfigDumpEvent.builder()
                .namespaceId(tenant).dataId(dataId).group(group).isBeta(isBeta).tag(tag)
                .lastModifiedTs(lastModified).handleIp(handleIp);

        // 如果是Beta发布，则查询并处理Beta配置信息。
        if (isBeta) {
            // 查询Beta配置信息。
            // beta发布，则dump数据，更新beta缓存
            ConfigInfo4Beta cf = persistService.findConfigInfo4Beta(dataId, group, tenant);
            // 根据查询结果动态构建ConfigDumpEvent，为dump操作做准备。
            build.remove(Objects.isNull(cf));
            build.betaIps(Objects.isNull(cf) ? null : cf.getBetaIps());
            build.content(Objects.isNull(cf) ? null : cf.getContent());

            // 执行配置dump操作，并返回结果。
            return DumpConfigHandler.configDump(build.build());
        } else {
            // 如果不是Beta发布，则根据是否有标签来进一步处理。
            if (StringUtils.isBlank(tag)) {
                // 查询普通配置信息。
                ConfigInfo cf = persistService.findConfigInfo(dataId, group, tenant);

                // 根据查询结果动态构建ConfigDumpEvent，为dump操作做准备。
                build.remove(Objects.isNull(cf));
                build.content(Objects.isNull(cf) ? null : cf.getContent());
                build.type(Objects.isNull(cf) ? null : cf.getType());

                // 执行配置dump操作，并返回结果。
                return DumpConfigHandler.configDump(build.build());
            } else {
                // 查询带标签的配置信息。
                ConfigInfo4Tag cf = persistService.findConfigInfo4Tag(dataId, group, tenant, tag);
                // 根据查询结果动态构建ConfigDumpEvent，为dump操作做准备。
                build.remove(Objects.isNull(cf));
                build.content(Objects.isNull(cf) ? null : cf.getContent());

                // 执行配置dump操作，并返回结果。
                return DumpConfigHandler.configDump(build.build());
            }
        }
    }
    
    final DumpService dumpService;
}
