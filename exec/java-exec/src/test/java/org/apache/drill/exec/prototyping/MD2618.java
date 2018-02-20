/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.prototyping;

import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.DrillTest;
import org.junit.Rule;
import org.junit.Test;

public class MD2618 extends DrillTest {

    public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/project/original";
    @Rule
    public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    @Test
    public void testProjectSimple() throws Exception {

        LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
                .logger("org.apache.drill", Level.TRACE).toConsole();

        try (//LogFixture logs = logBuilder.build();
             ClusterFixture cluster = ClusterFixture.builder(dirTestWatcher)
                                 .configProperty(ClassBuilder.CODE_DIR_OPTION, GENERATED_SOURCES_DIR)
                                 .configProperty(ExecConstants.BIT_RPC_TIMEOUT, 0)
                                 .configProperty(ExecConstants.USER_RPC_TIMEOUT, 0)
                                 .configProperty(ExecConstants.BIT_TIMEOUT, 0)
                                 .build();

            ClientFixture client = cluster.clientFixture()) {
            client.queryBuilder().sql("select employee_id as eid " +
                    "                          , employee_id + position_id as eidpluspid " +
                    "                   from cp.`employee.json` ").printCsv();

        }
    }
}
