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

package org.apache.drill.exec.expr.fn.output;

import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;

import java.util.List;

/**
 * Return type calculation implementation for functions with return type set as
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate.ReturnType#CONCAT}.
 */

public class OutputSizeEstimators {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutputSizeEstimators.class);

    static OutputSizeEstimator getOutputSizeEstimator(DrillFuncHolderExpr funcHolderExpr) {
        return funcHolderExpr.getHolder().getOutputSizeEstimator();
    }

    private static int adjustOutputSize(int outputSize, String prefix) {
        if (outputSize > Types.MAX_VARCHAR_LENGTH || outputSize < 0 /*overflow*/) {
            logger.warn(prefix + "Output size for expressions is too large, setting to MAX_VARCHAR_LENGTH");
            outputSize = Types.MAX_VARCHAR_LENGTH;
        }
        return outputSize;
    }

    public static class ConcatOutputSizeEstimator implements OutputSizeEstimator {

        public static final ConcatOutputSizeEstimator INSTANCE = new ConcatOutputSizeEstimator();

        /**
         * Defines function's output size estimate, which is caluclated as
         * sum of input sizes
         * If calculated size is greater than {@link Types#MAX_VARCHAR_LENGTH},
         * it is replaced with {@link Types#MAX_VARCHAR_LENGTH}.
         *
         * @param args
         * @return return type
         */
        @Override
        public int getEstimatedOutputSize(List<FixedLenExpr> args) {
            int outputSize = 0;
            for (FixedLenExpr expr : args) {
                outputSize += expr.getWidth();
            }
            outputSize = adjustOutputSize(outputSize, "ConcatOutputSizeEstimator:");
            return outputSize;
        }
    }

    public static class CloneOutputSizeEstimator implements OutputSizeEstimator {

        public static final CloneOutputSizeEstimator INSTANCE = new CloneOutputSizeEstimator();

        /**
         * Defines function's output size estimate, which is caluclated as
         * sum of input sizes
         * If calculated size is greater than {@link Types#MAX_VARCHAR_LENGTH},
         * it is replaced with {@link Types#MAX_VARCHAR_LENGTH}.
         *
         * @param args logical expressions
         * @return return type
         */
        @Override
        public int getEstimatedOutputSize(List<FixedLenExpr> args) {
            int outputSize = 0;
            if (args.size() != 1) {
                throw new IllegalArgumentException();
            }
            outputSize = args.get(0).getWidth();
            outputSize = adjustOutputSize(outputSize, "CloneOutputSizeEstimator");
            return outputSize;
        }
    }

}
