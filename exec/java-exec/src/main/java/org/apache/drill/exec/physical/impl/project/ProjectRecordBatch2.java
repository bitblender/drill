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

import com.carrotsearch.hppc.IntHashSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.UntypedNullHolder;
import org.apache.drill.exec.vector.UntypedNullVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import java.util.List;

public class ProjectRecordBatch2 extends AbstractSingleRecordBatch<Project> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch2.class);
  private Projector projector;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
  private List<FieldReference> complexFieldReferencesList;
  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount;

  private ResultSetLoaderImpl rsLoader;

  public static int codeGenCountHack = 0;

  private boolean first = true;
  private boolean wasNone = false; // whether a NONE iter outcome was already seen

  public static void UNIMPLEMENTED() {
    throw new RuntimeException("Unimplemented");
  }

  public ProjectRecordBatch2(final Project pop, final RecordBatch incoming, final FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
    rsLoader = new ResultSetLoaderImpl(container.getAllocator());
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  public void setOutgoingContainer(VectorContainer container) {
    this.container = container;
  }

  @Override
  protected void killIncoming(final boolean sendUpstream) {
    super.killIncoming(sendUpstream);
    hasRemainder = false;
  }

  @Override
  public IterOutcome innerNext() {
    if (wasNone) {
      return IterOutcome.NONE;
    }
    recordCount = 0;
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;
    }
    return super.innerNext();
  }

  @Override
  protected IterOutcome doWork() {
    if (wasNone) {
      return IterOutcome.NONE;
    }

    int incomingRecordCount = incoming.getRecordCount();

    if (first && incomingRecordCount == 0) {
      if (complexWriters != null) {
        UNIMPLEMENTED();
        IterOutcome next = null;
        while (incomingRecordCount == 0) {
          next = next(incoming);
          if (next == IterOutcome.OUT_OF_MEMORY) {
            outOfMemory = true;
            return next;
          } else if (next == IterOutcome.NONE) {
            // since this is first batch and we already got a NONE, need to set up the schema
            if (!doAlloc(0)) {
              outOfMemory = true;
              return IterOutcome.OUT_OF_MEMORY;
            }
            setValueCount(0);

            // Only need to add the schema for the complex exprs because others should already have
            // been setup during setupNewSchema
            for (FieldReference fieldReference : complexFieldReferencesList) {
              MaterializedField field = MaterializedField.create(fieldReference.getAsNamePart().getName(), UntypedNullHolder.TYPE);
              container.add(new UntypedNullVector(field, container.getAllocator()));
            }
            container.buildSchema(SelectionVectorMode.NONE);
            wasNone = true;
            return IterOutcome.OK_NEW_SCHEMA;
          } else if (next != IterOutcome.OK && next != IterOutcome.OK_NEW_SCHEMA) {
            return next;
          }
          incomingRecordCount = incoming.getRecordCount();
        }
        if (next == IterOutcome.OK_NEW_SCHEMA) {
          try {
            setupNewSchema();
          } catch (final SchemaChangeException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    first = false;

    container.zeroVectors();

    //if (!doAlloc(incomingRecordCount)) {
    //  outOfMemory = true;
    //  return IterOutcome.OUT_OF_MEMORY;
    //}

    final int outputRecords = projector.projectRecords2(0, incomingRecordCount, rsLoader, this);
    if (outputRecords < incomingRecordCount) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      this.recordCount = remainderIndex;
    } else {
      setValueCount(incomingRecordCount);
      for (final VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    return IterOutcome.OK;
  }

  private void handleRemainder() {
    final int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    //if (!doAlloc(remainingRecordCount)) {
    //  outOfMemory = true;
    // return;
    //}
    final int projRecords = projector.projectRecords2(remainderIndex, remainingRecordCount, rsLoader, this);
    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      this.recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      for (final VectorWrapper<?> v : incoming) {
        v.clear();
      }
      this.recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
  }

  private boolean doAlloc(int recordCount) {
    //Allocate vv in the allocationVectors.
    for (final ValueVector v : this.allocationVectors) {
      AllocationHelper.allocateNew(v, recordCount);
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null) {
      return true;
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

    return true;
  }

  private void setValueCount(final int count) {
    //for (final ValueVector v : allocationVectors) {
    //  final ValueVector.Mutator m = v.getMutator();
    //  m.setValueCount(count);
    //}

    if (complexWriters == null) {
      return;
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  /** hack to make ref and full work together... need to figure out if this is still necessary. **/
  private FieldReference getRef(final NamedExpression e) {
    return e.getRef();
  }

  private boolean isAnyWildcard(final List<NamedExpression> exprs) {
    for (final NamedExpression e : exprs) {
      if (isWildcard(e)) {
        return true;
      }
    }
    return false;
  }

  private boolean isWildcard(final NamedExpression ex) {
    if ( !(ex.getExpr() instanceof SchemaPath)) {
      return false;
    }
    final NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    return expr.getPath().contains(SchemaPath.WILDCARD);
  }

  private void setupNewSchemaFromInput(RecordBatch incomingBatch) throws SchemaChangeException {
    //if (allocationVectors != null) {
    //  for (final ValueVector v : allocationVectors) {
    //    v.clear();
    //  }
    //}
    //this.allocationVectors = Lists.newArrayList();

    if (complexWriters != null) {
      UNIMPLEMENTED();
      container.clear();
    } else {
      container.zeroVectors();
    }

    final List<NamedExpression> exprs = getExpressionList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    cg.getCodeGenerator().saveCodeForDebugging(true);

    final IntHashSet transferFieldIds = new IntHashSet();

    final boolean isAnyWildcard = isAnyWildcard(exprs);

    final ExpressionClassifier.ClassifierResult result = new ExpressionClassifier.ClassifierResult();
    final boolean classify = isClassificationNeeded(exprs);
    RowSetLoader rootWriter = rsLoader.writer();

    for (NamedExpression namedExpression : exprs) {
      result.clear();

      if (classify && namedExpression.getExpr() instanceof SchemaPath) {
        ExpressionClassifier.classifyExpr(namedExpression, incomingBatch, result);

        if (result.isStar) {
          handleWildCards(incomingBatch, collector, transfers, cg, result);
          continue;
        }
      } else {
        // For the columns which do not needed to be classified,
        // it is still necessary to ensure the output column name is unique
        result.outputNames = Lists.newArrayList();
        final String outputName = getRef(namedExpression).getRootSegment().getPath();
        ExpressionClassifier.addToResultMaps(outputName, result, true);
      }

      String outputName = getRef(namedExpression).getRootSegment().getPath();
      if (result != null && result.outputNames != null && result.outputNames.size() > 0) {
        boolean isMatched = false;
        for (int j = 0; j < result.outputNames.size(); j++) {
          if (!result.outputNames.get(j).isEmpty()) {
            outputName = result.outputNames.get(j);
            isMatched = true;
            break;
          }
        }

        if (!isMatched) {
          continue;
        }
      }

      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incomingBatch,
          collector, context.getFunctionRegistry(), true, unionTypeEnabled);
      final MaterializedField outputField = MaterializedField.create(outputName, expr.getMajorType());

      //KM_TBD: is the following sufficient ?
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if (expr instanceof ValueVectorReadExpression && incomingBatch.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE
          && !((ValueVectorReadExpression) expr).hasReadPath()
          && !isAnyWildcard
          && !transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0])) {

        final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        final TypedFieldId id = vectorRead.getFieldId();
        final ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        Preconditions.checkNotNull(incomingBatch);

        final FieldReference ref = getRef(namedExpression);
        final ValueVector vvOut = container.addOrGet(MaterializedField.create(ref.getLastSegment().getNameSegment().getPath(),
                                                                              vectorRead.getMajorType()), callBack);
        final TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
      } else if (expr instanceof DrillFuncHolderExpr &&
          ((DrillFuncHolderExpr) expr).getHolder().isComplexWriterFuncHolder()) {

        UNIMPLEMENTED();

        // Need to process ComplexWriter function evaluation.
        // Lazy initialization of the list of complex writers, if not done yet.
        if (complexWriters == null) {
          complexWriters = Lists.newArrayList();
        } else {
          complexWriters.clear();
        }

        // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
        ((DrillFuncHolderExpr) expr).getFieldReference(namedExpression.getRef());
        cg.addExpr(expr, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
        if (complexFieldReferencesList == null) {
          complexFieldReferencesList = Lists.newArrayList();
        }
        // save the field reference for later for getting schema when input is empty
        complexFieldReferencesList.add(namedExpression.getRef());
      } else {
        // need to do evaluation.
        //KM_TBD: SchemaChangeCallback
        //final ValueVector vector = container.addOrGet(outputField, callBack);
        //allocationVectors.add(vector);
        rootWriter.addColumn(outputField);

        //KM_TBD: Revisit when doing codegen changes
        //final TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
        //final boolean useSetSafe = !(vector instanceof FixedWidthVector);
        //final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        //cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);

        // We cannot do multiple transfers from the same vector. However we still need to instantiate the output vector.
        if (expr instanceof ValueVectorReadExpression) {
          UNIMPLEMENTED();
          final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          if (!vectorRead.hasReadPath()) {
            final TypedFieldId id = vectorRead.getFieldId();
            final ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();

            //vvIn.makeTransferPair(vector);
          }
        }
        logger.debug("Added eval for project expression.");
      }
    }

    try {
      CodeGenerator<Projector> codeGen = cg.getCodeGenerator();
      codeGen.plainJavaCapable(true);
      // Uncomment out this line to debug the generated code.
      // codeGen.saveCodeForDebugging(true);
      this.projector = context.getImplementationClass(codeGen);
//    } catch (ClassTransformationException | IOException e) {
//      throw new SchemaChangeException("Failure while attempting to load generated class", e);
//    }

      if (codeGenCountHack == 0) {
        codeGenCountHack++;
//        this.projector = new org.apache.drill.exec.test.generated.KMProjectorGen0();
      } else if (codeGenCountHack == 1) {
//        this.projector = new  org.apache.drill.exec.test.generated.KMProjectorGen2();
      }
      projector.setup(context, incomingBatch, this, transfers);

      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
  }

  private void handleWildCards(RecordBatch incomingBatch, ErrorCollector collector, List<TransferPair> transfers, ClassGenerator<Projector> cg, ExpressionClassifier.ClassifierResult result) throws SchemaChangeException {
    // The value indicates which wildcard we are processing now
    final Integer value = result.prefixMap.get(result.prefix);
    if (value != null && value == 1) {
      int k = 0;
      for (final VectorWrapper<?> wrapper : incomingBatch) {
        final ValueVector vvIn = wrapper.getValueVector();
        if (k > result.outputNames.size() - 1) {
          assert false;
        }
        final String name = result.outputNames.get(k++);  // get the renamed column names
        if (name.isEmpty()) {
          continue;
        }

        if (isImplicitFileColumn(vvIn)) {
          continue;
        }

        final FieldReference ref = new FieldReference(name);
        final ValueVector vvOut = container.addOrGet(MaterializedField.create(ref.getAsNamePart().getName(), vvIn.getField().getType()), callBack);
        final TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
      }
    } else if (value != null && value > 1) { // subsequent wildcards should do a copy of incoming valuevectors
      int k = 0;
      for (final VectorWrapper<?> wrapper : incomingBatch) {
        final ValueVector vvIn = wrapper.getValueVector();
        final SchemaPath originalPath = SchemaPath.getSimplePath(vvIn.getField().getName());
        if (k > result.outputNames.size() - 1) {
          assert false;
        }
        final String name = result.outputNames.get(k++);  // get the renamed column names
        if (name.isEmpty()) {
          continue;
        }

        if (isImplicitFileColumn(vvIn)) {
          continue;
        }

        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(originalPath, incomingBatch, collector, context.getFunctionRegistry() );
        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format("Failure while trying to materialize incomingBatch schema.  Errors:\n %s.", collector.toErrorString()));
        }

        final MaterializedField outputField = MaterializedField.create(name, expr.getMajorType());
        final ValueVector vv = container.addOrGet(outputField, callBack);
        allocationVectors.add(vv);
        final TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
        final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
        final HoldingContainer hc = cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
      }
    }
  }


  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    setupNewSchemaFromInput(this.incoming);
    if (container.isSchemaChanged()) {
      container.buildSchema(SelectionVectorMode.NONE);
      return true;
    } else {
      return false;
    }
  }

  private boolean isImplicitFileColumn(ValueVector vvIn) {
    return ColumnExplorer.initImplicitFileColumns(context.getOptions()).get(vvIn.getField().getName()) != null;
  }

  private List<NamedExpression> getExpressionList() {
    if (popConfig.getExprs() != null) {
      return popConfig.getExprs();
    }

    final List<NamedExpression> exprs = Lists.newArrayList();
    for (final MaterializedField field : incoming.getSchema()) {
      String fieldName = field.getName();
      if (Types.isComplex(field.getType()) || Types.isRepeated(field.getType())) {
        final LogicalExpression convertToJson = FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "JSON",
                                                            SchemaPath.getSimplePath(fieldName), ExpressionPosition.UNKNOWN);
        final String castFuncName = CastFunctions.getCastFunc(MinorType.VARCHAR);
        final List<LogicalExpression> castArgs = Lists.newArrayList();
        castArgs.add(convertToJson);  //input_expr
        // implicitly casting to varchar, since we don't know actual source length, cast to undefined length, which will preserve source length
        castArgs.add(new ValueExpressions.LongExpression(Types.MAX_VARCHAR_LENGTH, null));
        final FunctionCall castCall = new FunctionCall(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
        exprs.add(new NamedExpression(castCall, new FieldReference(fieldName)));
      } else {
        exprs.add(new NamedExpression(SchemaPath.getSimplePath(fieldName), new FieldReference(fieldName)));
      }
    }
    return exprs;
  }

  private boolean isClassificationNeeded(final List<NamedExpression> exprs) {
    boolean needed = false;
    for (NamedExpression ex : exprs) {
      if (!(ex.getExpr() instanceof SchemaPath)) {
        continue;
      }
      final NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
      final NameSegment ref = ex.getRef().getRootSegment();
      final boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
      final boolean exprContainsStar = expr.getPath().contains(SchemaPath.WILDCARD);

      if (refHasPrefix || exprContainsStar) {
        needed = true;
        break;
      }
    }
    return needed;
  }

  /**
   * Handle Null input specially when Project operator is for query output. This happens when input return 0 batch
   * (returns a FAST NONE directly).
   *
   * <p>
   * Project operator has to return a batch with schema derived using the following 3 rules:
   * </p>
   * <ul>
   *  <li>Case 1:  *  ==>  expand into an empty list of columns. </li>
   *  <li>Case 2:  regular column reference ==> treat as nullable-int column </li>
   *  <li>Case 3:  expressions => Call ExpressionTreeMaterialization over an empty vector contain.
   *           Once the expression is materialized without error, use the output type of materialized
   *           expression. </li>
   * </ul>
   *
   * <p>
   * The batch is constructed with the above rules, and recordCount = 0.
   * Returned with OK_NEW_SCHEMA to down-stream operator.
   * </p>
   */
  @Override
  protected IterOutcome handleNullInput() {
    if (! popConfig.isOutputProj()) {
      return super.handleNullInput();
    }

    VectorContainer emptyVC = new VectorContainer();
    emptyVC.buildSchema(SelectionVectorMode.NONE);
    RecordBatch emptyIncomingBatch = new SimpleRecordBatch(emptyVC, context);

    try {
      setupNewSchemaFromInput(emptyIncomingBatch);
    } catch (SchemaChangeException e) {
      kill(false);
      logger.error("Failure during query", e);
      context.getExecutorState().fail(e);
      return IterOutcome.STOP;
    }

    doAlloc(0);
    container.buildSchema(SelectionVectorMode.NONE);
    wasNone = true;
    return IterOutcome.OK_NEW_SCHEMA;
  }

}

