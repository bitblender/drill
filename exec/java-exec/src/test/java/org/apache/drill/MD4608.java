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
package org.apache.drill;


import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;


public class MD4608 extends DrillTest {

  public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
  @Rule public final TestRule TIMEOUT = new DisableOnDebug(TestTools.getTimeoutRule(10_000_000));

  @Test
  public void testMD4608() throws Exception {

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
      cluster.defineWorkspace("dfs", "data", "/Users/karthik/work/bugs/", "csv");
      //String sql = "select BooleanValue as foo from dfs.data.`MD4608/alltypes_small_1MB_1GB.parquet`";
      String sql = "select age(cast(to_date(cast(cume_dist(length(substr(concat(BinaryValue, VarcharValue),10,8000))) over (order by Index) as Integer)) as Timestamp), '2018-05-10') IntervalSecondValuea, 'Fq7gIk2x1FPI8nQUXG7SqBcXKx7ocIBa17CNC9KWtrlW7gZykOfHVQhipHs5C5DDAZWhb0EwxnEX16TA7Eb6HTC1 wMr3w0sPhVG' " +
              "NewCharacterValue, Index, BigIntValue, BooleanValue, DateValue, FloatValue, DoubleValue, NullValue, IntegerValue, TimeValue, TimestampValue, IntervalYearValue, IntervalDayValue, IntervalSecondValue, age(cast(to_date(cast(cume_dist(length(substr(concat(BinaryValue, VarcharValue),10,8000))) over " +
              "(order by Index) as Integer)) as Timestamp), '2018-05-10') IntervalSecondValueb from (select * from dfs.data.`MD4608/alltypes_small_1MB_1GB.parquet` order by BigIntvalue) where Index = 1";
      client.queryBuilder().sql(sql).printCsv();
    }
  }
}