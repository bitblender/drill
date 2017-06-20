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

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MapRSaslACLProvider implements ACLProvider {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRSaslACLProvider.class);
    static ArrayList<ACL> DEFAULT_ACL = new ArrayList(Collections.singletonList(new ACL(1, new Id("sasl", "mapr"))));

    final String clusterName;
    final String drillZkRoot;
    final String drillClusterPath;

    public MapRSaslACLProvider(String clusterName, String drillZKRoot) {
        this.clusterName = clusterName;
        this.drillZkRoot = drillZKRoot;
        this.drillClusterPath = this.drillZkRoot +  this.clusterName ;
    }

    public List<ACL> getDefaultAcl() {
        //return Ids.OPEN_ACL_UNSAFE;
        return DEFAULT_ACL;
    }

    public List<ACL> getAclForPath(String path) {
        logger.trace("MapRSaslACLProvider: getAclForPath " + path);
        if(path.equals(drillClusterPath)) {
            logger.trace("MapRSaslACLProvider: getAclForPath drillClusterPath " + drillClusterPath);
            return Ids.READ_ACL_UNSAFE;
        }
        return DEFAULT_ACL;
    }

}
