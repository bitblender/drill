package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.RepeatedVariableWidthVectorLike;

import java.util.HashMap;
import java.util.Map;

public class ProjectMemoryManager extends RecordBatchMemoryManager {

    RecordBatch incomingBatch = null;

    public void setIncomingBatch(RecordBatch recordBatch) { incomingBatch = recordBatch; }

    public static void Unimplemented(String message) {
        throw new IllegalStateException(message);
    }

    enum WidthType {
        FIXED,
        VARIABLE
    };

    enum OutputColumnType {
        TRANSFER,
        NEW
    }

    int rowWidth = 0;

    Map<String, ColumnSizeInfo> outputColumns;


    public ProjectMemoryManager(int configuredOutputSize) {
        super(configuredOutputSize);
        outputColumns = new HashMap<>();
    }



    static boolean isFixedWidth(ValueVector vv) {
       assert (vv instanceof FixedWidthVector) == !(vv instanceof VariableWidthVector
                                                    || vv instanceof RepeatedVariableWidthVectorLike);
       return (vv instanceof FixedWidthVector);
    }

    /**
     * Valid only for fixed width vectors
     * @param vv
     * @return
     */
    static int getWidthOfFixedWidthField(ValueVector vv) {
        return vv.getPayloadByteCount(1);
    }

    class ColumnSizeInfo {
        MaterializedField materializedField;
        LogicalExpression outputExpression;
        int width;
        WidthType widthType;
        OutputColumnType outputColumnType;

        ColumnSizeInfo (ValueVector vv,
                        LogicalExpression logicalExpression,
                        OutputColumnType outputColumnType,
                        WidthType widthType,
                        int fieldWidth) {
            this.materializedField = vv.getField();
            this.outputExpression = logicalExpression;
            this.width = fieldWidth;
            this.outputColumnType = outputColumnType;
            this.widthType = widthType;
        }

        public String getName() { return materializedField.getName(); }

    }

    void addField(ValueVector vv, LogicalExpression logicalExpression,
                  OutputColumnType outputColumnType) {
            if(isFixedWidth(vv)) {
                addFixedWidthField(vv, outputColumnType);
            } else {
                ProjectMemoryManager.Unimplemented("Project Batch sizing only implemented for FW types");
            }
    }

    void addFixedWidthField(ValueVector vv, OutputColumnType outputColumnType) {
        assert isFixedWidth(vv);
        int fixedFieldWidth = getWidthOfFixedWidthField(vv);
        ColumnSizeInfo columnSizeInfo = new ColumnSizeInfo(vv, null, outputColumnType, WidthType.FIXED,
                                                           fixedFieldWidth);
        outputColumns.put(columnSizeInfo.getName(), columnSizeInfo);
        rowWidth += columnSizeInfo.width;
    }

    @Override
    public void update() {
        setOutputRowCount(getOutputBatchSize(), rowWidth);
        RecordBatchSizer batchSizer = new RecordBatchSizer(incomingBatch);
        setRecordBatchSizer(batchSizer);
        int outPutRowCount = Math.min(getOutputRowCount(), batchSizer.rowCount());
        setOutputRowCount(outPutRowCount);
    }
}
