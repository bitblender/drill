/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.coord.zk;

import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public class ZKACLProvider implements ACLProvider {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKACLProvider.class);
    static ImmutableList<ACL> DEFAULT_ACL = new ImmutableList.Builder<ACL>()
                                              .addAll(Ids.CREATOR_ALL_ACL.iterator())
                                              .build();
    static ImmutableList<ACL> DRILL_CLUSTER_ACL = new ImmutableList.Builder<ACL>()
                                                .addAll(Ids.READ_ACL_UNSAFE.iterator())
                                                .addAll(Ids.CREATOR_ALL_ACL.iterator())
                                                .build();
    final String clusterName;
    final String drillZkRoot;
    final String drillClusterPath;

    public ZKACLProvider(String clusterName, String drillZKRoot) {
        this.clusterName = clusterName;
        this.drillZkRoot = drillZKRoot;
        this.drillClusterPath = "/" + this.drillZkRoot + "/" +  this.clusterName ;
    }

    public List<ACL> getDefaultAcl() {
        return DEFAULT_ACL;
    }

    public List<ACL> getAclForPath(String path) {
        logger.trace("ZKACLProvider: getAclForPath " + path);
        if(path.equals(drillClusterPath)) {
            logger.trace("ZKACLProvider: getAclForPath drillClusterPath " + drillClusterPath);
            return DRILL_CLUSTER_ACL;
        }
        return DEFAULT_ACL;
    }

}
