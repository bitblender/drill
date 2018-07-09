/*
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
package org.apache.drill;
import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.QueryTestUtil;


import org.apache.drill.test.LogFixture;
import org.junit.Rule;
import org.junit.Test;

public class MD4555 extends DrillTest {

  public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void testMD4555() throws Exception {

    LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder().logger(VectorUtil.class, Level.INFO).toConsole();

    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = ClusterFixture.builder(dirTestWatcher)
                 .configProperty(ClassBuilder.CODE_DIR_OPTION, GENERATED_SOURCES_DIR)
                 .configProperty(ExecConstants.BIT_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.USER_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.BIT_TIMEOUT, 0)
                 .configProperty(QueryTestUtil.TEST_QUERY_PRINTING_SILENT, false)


                 .configProperty(ExecConstants.PROJECT_OUTPUT_BATCH_SIZE, 131072)
                 .configProperty(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
                 .configProperty(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 1)

                 .build();

         ClientFixture client = cluster.clientFixture()) {
         cluster.defineWorkspace("dfs", "data", "/Users/karthik/work/bugs/MD-4555", "csv");
         String sql = "select * from " +
                      "(select case when false then c.CharacterValuea else i.IntegerValuea end IntegerValuea, " +
                              "case when false then c.CharacterValueb else i.IntegerValueb end IntegerValueb, " +
                              "case when false then c.CharacterValuec else i.IntegerValuec end IntegerValuec, " +
                              "case when false then c.CharacterValued else i.IntegerValued end IntegerValued, " +
                              "case when false then c.CharacterValuee else i.IntegerValuee end IntegerValuee from " +
                              "(select * from dfs.data.`character5_1MB.parquet` " +
                              "order by CharacterValuea) c, dfs.data.`integer5_1MB.parquet` i " +
                              "where i.Index = c.Index and c.CharacterValuea = '1234567890123100') limit 10";
         //String sql = "select * from dfs.data.`integer5_1MB.parquet`";
         //String sql = "select * from cp.`employee.json`";

         client.queryBuilder().sql(sql).printCsv();
    }
  }
}
