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
package org.apache.drill.exec.vector;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.NullableVarCharVector.Accessor;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSplitAndTransfer {
  @Test
  public void test() throws Exception {
    final DrillConfig drillConfig = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConfig);
    final MaterializedField field = MaterializedField.create("field", Types.optional(MinorType.VARCHAR));
    final NullableVarCharVector varCharVector = new NullableVarCharVector(field, allocator);
    varCharVector.allocateNew(10000, 1000);

    final int valueCount = 500;
    final String[] compareArray = new String[valueCount];

    final NullableVarCharVector.Mutator mutator = varCharVector.getMutator();
    for (int i = 0; i < valueCount; i += 3) {
      final String s = String.format("%010d", i);
      mutator.set(i, s.getBytes());
      compareArray[i] = s;
    }
    mutator.setValueCount(valueCount);

    final TransferPair tp = varCharVector.getTransferPair(allocator);
    final NullableVarCharVector newVarCharVector = (NullableVarCharVector) tp.getTo();
    final Accessor accessor = newVarCharVector.getAccessor();
    final int[][] startLengths = {{0, 201}, {201, 200}, {401, 99}};

    for (final int[] startLength : startLengths) {
      final int start = startLength[0];
      final int length = startLength[1];
      tp.splitAndTransfer(start, length);
      newVarCharVector.getMutator().setValueCount(length);
      for (int i = 0; i < length; i++) {
        final boolean expectedSet = ((start + i) % 3) == 0;
        if (expectedSet) {
          final byte[] expectedValue = compareArray[start + i].getBytes();
          assertFalse(accessor.isNull(i));
          assertArrayEquals(expectedValue, accessor.get(i));
        } else {
          assertTrue(accessor.isNull(i));
        }
      }
      newVarCharVector.clear();
    }

    varCharVector.close();
    allocator.close();
  }

  @Test
  public void testBitVector() throws Exception {
    final DrillConfig drillConfig = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConfig);
    final MaterializedField field = MaterializedField.create("field", Types.optional(MinorType.BIT));
    final BitVector bitVector = new BitVector(field, allocator);
    bitVector.allocateNew(3443);

    final int valueCount = 3443;
    final int[] compareArray = new int[valueCount];

    final BitVector.Mutator mutator = bitVector.getMutator();
    for (int i = 0; i < valueCount; i ++) {
      mutator.set(i, 1);
      compareArray[i] = 1;
    }
    mutator.setValueCount(valueCount);

    final TransferPair tp = bitVector.getTransferPair(allocator);
    final BitVector newBitVector = (BitVector) tp.getTo();
    final BitVector.Accessor accessor = newBitVector.getAccessor();
    final int[][] startLengths = {{0, 2047}, {2047, 1396}};

    for (final int[] startLength : startLengths) {
      final int start = startLength[0];
      final int length = startLength[1];
      tp.splitAndTransfer(start, length);
      newBitVector.getMutator().setValueCount(length);
      for (int i = 0; i < length; i++) {
          final int expectedValue = compareArray[start + i];
          System.out.println("i " + i + ", exp " + expectedValue + ", act " + accessor.get(i));
          assertEquals(expectedValue, accessor.get(i));
      }
      newBitVector.clear();
    }

    bitVector.close();
    allocator.close();
  }
}
