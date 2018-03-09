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
package org.apache.drill.exec.physical.impl.project;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ExternalColumnSizerImpl;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import javax.inject.Named;
import java.util.List;

public abstract class ProjectorTemplate implements Projector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectorTemplate.class);

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;

  public ProjectorTemplate() {
  }

  @Override
  public final int projectRecords(int startIndex, final int recordCount, int firstOutputIndex) {
    switch (svMode) {
    case FOUR_BYTE:
      throw new UnsupportedOperationException();

    case TWO_BYTE:
      final int count = recordCount;
      for (int i = 0; i < count; i++, firstOutputIndex++) {
        try {
          doEval(vector2.getIndex(i), firstOutputIndex);
        } catch (SchemaChangeException e) {
          throw new UnsupportedOperationException(e);
        }
      }
      return recordCount;

    case NONE:
      final int countN = recordCount;
      int i;
      for (i = startIndex; i < startIndex + countN; i++, firstOutputIndex++) {
        try {
          doEval(i, firstOutputIndex);
        } catch (SchemaChangeException e) {
          throw new UnsupportedOperationException(e);
        }
      }
      if (i < startIndex + recordCount || startIndex > 0) {
        for (TransferPair t : transfers) {
          t.splitAndTransfer(startIndex, i - startIndex);
        }
        return i - startIndex;
      }
      for (TransferPair t : transfers) {
          t.transfer();
      }
      return recordCount;

    default:
      throw new UnsupportedOperationException();
    }
  }


  public final int projectRecords2(int startIndex, final int recordCount,
                                   ResultSetLoader rsLoader, ProjectRecordBatch2 batch) {
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException();

      case TWO_BYTE:

        ProjectRecordBatch2.UNIMPLEMENTED();
        return recordCount;

      case NONE:
        rsLoader.setTargetRowCount(100); // set a dummy limit
        rsLoader.startBatch();
        RowSetLoader rootWriter = rsLoader.writer();
        final ExternalColumnSizerImpl externalColumnSizer = new ExternalColumnSizerImpl(batch.getOutgoingContainer());
        rsLoader.setExternalColumnSizer(externalColumnSizer);
        VectorContainer allocedColumsContainer = null;
        int index;
        for (index = startIndex; index < startIndex + recordCount; index++) {
          try {
            if (rootWriter.isFull()) {
              break;
            } else {
              // Equivalent of generated code
              rootWriter.start();
              doEval(index, rsLoader);
              rootWriter.save();
            }
          } catch (SchemaChangeException e) {
            throw new UnsupportedOperationException(e);
          }
        }
        allocedColumsContainer = rsLoader.harvest();
        assert index >= startIndex;
        final int rowsProcessed = index - startIndex;
        assert rowsProcessed <= recordCount;
        // transfers need a split
        for (TransferPair t : transfers) {
          if (rowsProcessed < recordCount || startIndex > 0) {
            t.splitAndTransfer(startIndex, rowsProcessed);
          } else {
            t.transfer();
          }
        }
        //merge of alloced and transferred rows

        final VectorContainer outgoingContainer = batch.getOutgoingContainer();
        //KM_TBD: Hack: outgoingContainer only has transfer pairs. set record count to match the alloced columns
        outgoingContainer.setRecordCount(allocedColumsContainer.getRecordCount());
        final VectorContainer mergedContainer = outgoingContainer.union(allocedColumsContainer);
        batch.setOutgoingContainer(mergedContainer);
        assert rowsProcessed <= recordCount;
        return rowsProcessed;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, List<TransferPair> transfers)  throws SchemaChangeException{

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
    case FOUR_BYTE:
      this.vector4 = incoming.getSelectionVector4();
      break;
    case TWO_BYTE:
      this.vector2 = incoming.getSelectionVector2();
      break;
    }
    this.transfers = ImmutableList.copyOf(transfers);
    doSetup(context, incoming, outgoing);
  }

  public void doEval(int inIndex, ResultSetLoader rsLoader)
          throws SchemaChangeException { }


  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("incoming") RecordBatch incoming,
                               @Named("outgoing") RecordBatch outgoing)
                       throws SchemaChangeException;
  public abstract void doEval(@Named("inIndex") int inIndex,
                              @Named("outIndex") int outIndex)
                       throws SchemaChangeException;

}
