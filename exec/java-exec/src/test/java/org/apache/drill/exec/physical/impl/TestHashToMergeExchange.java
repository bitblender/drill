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
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.junit.experimental.categories.Category;

@Category(OperatorTest.class)
public class TestHashToMergeExchange extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHashToRandomExchange.class);

  public void twoBitTwoExchangeTwoEntryBatchSizingRun(int batchSizeLimit) throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.asCharSource(DrillFileUtils.getResourceAsFile("/sender/hash_merge_exchange.json"),
                      Charsets.UTF_8).read());
      int count = 0;
      final RecordBatchLoader loader = new RecordBatchLoader(client.getAllocator());
      for (QueryDataBatch b : results) {
        int recordCount = (b.getHeader().getRowCount());
        if (recordCount != 0) {
          // convert QueryDataBatch to a VectorContainer
          loader.load(b.getHeader().getDef(), b.getData());
          final VectorContainer container = loader.getContainer();
          container.setRecordCount(recordCount);
          // check if the size is within the batchSizeLimit
          RecordBatchSizer recordBatchSizer = new RecordBatchSizer(container);
          assertTrue(recordBatchSizer.getNetBatchSize() <= batchSizeLimit);
          count += recordCount;
        }
        b.release();
      }
      assertEquals(100000 * 5, count);
    }
  }

  // Test UnionExchange BatchSizing
  @Test
  public void twoBitHashToMergeExchangeRunBatchSizing() throws Exception {
    twoBitTwoExchangeTwoEntryBatchSizingRun(64 * 1024);
    twoBitTwoExchangeTwoEntryBatchSizingRun(512 * 1024);
    twoBitTwoExchangeTwoEntryBatchSizingRun(1024 * 1024);
  }

}