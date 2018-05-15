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

import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.AbstractExecExprVisitor;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.output.OutputWidthCalculator;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FunctionCallExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;

public class OutputWidthVisitor extends AbstractExecExprVisitor<OutputWidthExpression, OutputWidthVisitorState,
        RuntimeException> {

    @Override
    public OutputWidthExpression visitFunctionHolderExpression(FunctionHolderExpression holderExpr,
                                                               OutputWidthVisitorState state) throws RuntimeException {
        OutputWidthExpression fixedWidth = getFixedLenExpr(holderExpr.getMajorType());
        if (fixedWidth != null) { return fixedWidth; }
        //KM_TBD Handling for HiveFunctionHolder
        final DrillFuncHolder holder = ((DrillFuncHolderExpr) holderExpr).getHolder();
        //KM_TBD: move constant val to a fun in template
        // Use the user-provided estimate
        int estimate = holder.variableOuputSizeEstimate();
        if (estimate != FunctionTemplate.VARIABLE_OUTPUT_SIZE_ESTIMATE_DEFAULT) {
            return new FixedLenExpr(estimate);
        }
        OutputWidthCalculator estimator = holder.getOutputSizeCalculator();
        final int argSize = holderExpr.args.size();
        ArrayList<OutputWidthExpression> arguments = null;
        if (argSize != 0) {
            arguments = new ArrayList<>(argSize);
            for (LogicalExpression expr : holderExpr.args) {
                arguments.add(expr.accept(this, state));
            }
        }
        return new FunctionCallExpr(holderExpr, estimator, arguments);
    }

    @Override
    public OutputWidthExpression visitValueVectorWriteExpression(ValueVectorWriteExpression writeExpr,
                                                                 OutputWidthVisitorState state) throws RuntimeException {
        TypedFieldId fieldId = writeExpr.getFieldId();
        ProjectMemoryManager manager = state.getManager();
        OutputWidthExpression outputExpr = null;
        if (manager.isFixedWidth(fieldId)) {
            manager.addFixedWidthField(fieldId, state.getOutputColumnType());
            return null;
        } else {
            LogicalExpression writeArg = writeExpr.getChild();
            outputExpr = writeArg.accept(this, state);
        }
        return outputExpr;
    }

    @Override
    public OutputWidthExpression visitValueVectorReadExpression(ValueVectorReadExpression readExpr,
                                                                OutputWidthVisitorState state) throws RuntimeException {
        return new VarLenReadExpr(readExpr);
    }

    @Override
    public OutputWidthExpression visitQuotedStringConstant(ValueExpressions.QuotedString quotedString,
                                                           OutputWidthVisitorState state) throws RuntimeException {
        return new FixedLenExpr(quotedString.getString().length());
    }

    @Override
    public OutputWidthExpression visitUnknown(LogicalExpression logicalExpression, OutputWidthVisitorState state) {
        OutputWidthExpression fixedLenExpr = getFixedLenExpr(logicalExpression.getMajorType());
        if (fixedLenExpr != null) { return fixedLenExpr; }
        return null;
    }

    @Override
    public OutputWidthExpression visitFixedLenExpr(FixedLenExpr fixedLenExpr, OutputWidthVisitorState state)
            throws RuntimeException {
        return fixedLenExpr;
    }

    @Override
    public OutputWidthExpression visitVarLenReadExpr(VarLenReadExpr varLenReadExpr, OutputWidthVisitorState state)
                                                        throws RuntimeException {
        String columnName = varLenReadExpr.getName();
        if (columnName == null) {
            TypedFieldId fieldId = varLenReadExpr.getReadExpression().getTypedFieldId();
            ValueVector vv = state.manager.getIncomingValueVector(fieldId);
            columnName =  vv.getField().getName();
        }
        int columnWidth = state.manager.getColumnSize(columnName).getDataSizePerEntry();
        return new FixedLenExpr(columnWidth);
    }

    @Override
    public OutputWidthExpression visitFunctionCallExpr(FunctionCallExpr functionCallExpr, OutputWidthVisitorState state)
                                                        throws RuntimeException {
        ArrayList<OutputWidthExpression> args = functionCallExpr.getArgs();
        ArrayList<FixedLenExpr> estimatedArgs = null;

        if (args != null && args.size() != 0) {
            estimatedArgs = new ArrayList<>(args.size());
            for (OutputWidthExpression expr : args) {
                FixedLenExpr fixedLenExpr = (FixedLenExpr) expr.accept(this, state);
                estimatedArgs.add(fixedLenExpr);
            }
        }
        OutputWidthCalculator estimator = functionCallExpr.getEstimator();
        int estimatedSize = estimator.getOutputWidth(estimatedArgs);
        return new FixedLenExpr(estimatedSize);
    }

    private OutputWidthExpression getFixedLenExpr(MajorType majorType) {
        MajorType type = majorType;
        if (Types.isFixedWidthType(type)) {
            int fixedWidth = ProjectMemoryManager.getWidthOfFixedWidthType(type);
            return new OutputWidthExpression.FixedLenExpr(fixedWidth);
        }
        return null;
    }

}
