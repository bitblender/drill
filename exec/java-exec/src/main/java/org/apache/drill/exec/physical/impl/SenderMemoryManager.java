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

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.vector.ValueVector;

public class SenderMemoryManager extends RecordBatchMemoryManager {

  public static enum State {
    UNINITIALIZED,
    SETUP,
    INITIALIZED
  }

  private RecordBatch incomingBatch;
  private int batchSizeLimit;
  RecordBatchSizer batchSizer;
  private State state = State.UNINITIALIZED;

  public State getState() { return state; }
  public void setState(State state) { this.state = state; }

  public SenderMemoryManager(int batchSizeLimit, RecordBatch incomingBatch) {
    super(batchSizeLimit);
    this.batchSizeLimit = batchSizeLimit;
    this.incomingBatch = incomingBatch;
  }

  public void update() {
    batchSizer = new RecordBatchSizer(incomingBatch);
    setRecordBatchSizer(batchSizer);
    int outputRowCount = computeOutputRowCount(batchSizeLimit, batchSizer.getNetRowWidth());
    outputRowCount = Math.min(outputRowCount, batchSizer.rowCount());
    setOutputRowCount(outputRowCount);
  }

  public void allocate(ValueVector vector, int rowCount) {
    RecordBatchSizer.ColumnSize columnSize =  batchSizer.getColumn(vector.getField().getName());
    columnSize.allocateVector(vector, rowCount);
  }
}
