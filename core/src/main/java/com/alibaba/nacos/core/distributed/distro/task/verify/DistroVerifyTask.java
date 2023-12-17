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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyTask implements Runnable {
    
    private final ServerMemberManager serverMemberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    public DistroVerifyTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder) {
        this.serverMemberManager = serverMemberManager;
        this.distroComponentHolder = distroComponentHolder;
    }

    //书签 注册中心 服务端 集群数据不一致 校验
    /**
     * 其他非责任节点通过PUT /v1/ns/distro/checksum接收VERIFY Distro数据。
     * 非责任节点DistroConsistencyServiceImpl#onReceiveChecksums结合当前节点DataStore中的数据，比对出需要更新和需要删除的服务。
     * 对需要删除的服务，从DataSore和ServiceManager中删除。
     * 对需要更新的服务，需要调用GET /v1/ns/distro/datum反查查询责任节点获取服务对应注册表信息（从DataStore中查询），更新DataStore和ServiceManager中的注册信息。
     *
     * 参考 https://juejin.cn/post/6994258991094169614#heading-18
     * */
    @Override
    public void run() {
        try {
            List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("server list is: {}", targetServer);
            }
            for (String each : distroComponentHolder.getDataStorageTypes()) {
                verifyForDataStorage(each, targetServer);
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
        }
    }
    
    private void verifyForDataStorage(String type, List<Member> targetServer) {
        DistroData distroData = distroComponentHolder.findDataStorage(type).getVerifyData();
        if (null == distroData) {
            return;
        }
        distroData.setType(DataOperation.VERIFY);
        for (Member member : targetServer) {
            try {
                distroComponentHolder.findTransportAgent(type).syncVerifyData(distroData, member.getAddress());
            } catch (Exception e) {
                Loggers.DISTRO.error(String
                        .format("[DISTRO-FAILED] verify data for type %s to %s failed.", type, member.getAddress()), e);
            }
        }
    }
}
