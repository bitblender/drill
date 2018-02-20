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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.HashMap;
import java.util.List;

public class ExpressionClassifier {

    private static final String EMPTY_STRING = "";

    static class ClassifierResult {
        public boolean isStar = false;
        public List<String> outputNames;
        public String prefix = "";
        public HashMap<String, Integer> prefixMap = Maps.newHashMap();
        public CaseInsensitiveMap outputMap = new CaseInsensitiveMap();
        private final CaseInsensitiveMap sequenceMap = new CaseInsensitiveMap();

        public void clear() {
            isStar = false;
            prefix = "";
            if (outputNames != null) {
                outputNames.clear();
            }

            // note:  don't clear the internal maps since they have cumulative data..
        }
    }

    private static String getUniqueName(final String name, final ClassifierResult result) {
        final Integer currentSeq = (Integer) result.sequenceMap.get(name);
        if (currentSeq == null) { // name is unique, so return the original name
            final Integer n = -1;
            result.sequenceMap.put(name, n);
            return name;
        }
        // create a new name
        final Integer newSeq = currentSeq + 1;
        final String newName = name + newSeq;
        result.sequenceMap.put(name, newSeq);
        result.sequenceMap.put(newName, -1);

        return newName;
    }


    /**
     * Helper method to ensure unique output column names. If allowDupsWithRename is set to true, the original name
     * will be appended with a suffix number to ensure uniqueness. Otherwise, the original column would not be renamed even
     * even if it has been used
     *
     * @param origName            the original input name of the column
     * @param result              the data structure to keep track of the used names and decide what output name should be
     *                            to ensure uniqueness
     * @param allowDupsWithRename if the original name has been used, is renaming allowed to ensure output name unique
     */
    public static void addToResultMaps(final String origName, final ClassifierResult result, final boolean allowDupsWithRename) {
        String name = origName;
        if (allowDupsWithRename) {
            name = getUniqueName(origName, result);
        }
        if (!result.outputMap.containsKey(name)) {
            result.outputNames.add(name);
            result.outputMap.put(name,  name);
        } else {
            result.outputNames.add(EMPTY_STRING);
        }
    }


    public static void classifyExpr(final NamedExpression ex, final RecordBatch incoming, final ClassifierResult result)  {
        final NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
        final NameSegment ref = ex.getRef().getRootSegment();
        final boolean exprHasPrefix = expr.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
        final boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
        final boolean exprIsStar = expr.getPath().equals(SchemaPath.WILDCARD);
        final boolean refContainsStar = ref.getPath().contains(SchemaPath.WILDCARD);
        final boolean exprContainsStar = expr.getPath().contains(SchemaPath.WILDCARD);
        final boolean refEndsWithStar = ref.getPath().endsWith(SchemaPath.WILDCARD);

        String exprPrefix = EMPTY_STRING;
        String exprSuffix = expr.getPath();

        if (exprHasPrefix) {
            // get the prefix of the expr
            final String[] exprComponents = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
            assert(exprComponents.length == 2);
            exprPrefix = exprComponents[0];
            exprSuffix = exprComponents[1];
            result.prefix = exprPrefix;
        }

        boolean exprIsFirstWildcard = false;
        if (exprContainsStar) {
            result.isStar = true;
            final Integer value = result.prefixMap.get(exprPrefix);
            if (value == null) {
                final Integer n = 1;
                result.prefixMap.put(exprPrefix, n);
                exprIsFirstWildcard = true;
            } else {
                final Integer n = value + 1;
                result.prefixMap.put(exprPrefix, n);
            }
        }

        final int incomingSchemaSize = incoming.getSchema().getFieldCount();

        // input is '*' and output is 'prefix_*'
        if (exprIsStar && refHasPrefix && refEndsWithStar) {
            final String[] components = ref.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
            assert(components.length == 2);
            final String prefix = components[0];
            result.outputNames = Lists.newArrayList();
            for (final VectorWrapper<?> wrapper : incoming) {
                final ValueVector vvIn = wrapper.getValueVector();
                final String name = vvIn.getField().getName();

                // add the prefix to the incoming column name
                final String newName = prefix + StarColumnHelper.PREFIX_DELIMITER + name;
                addToResultMaps(newName, result, false);
            }
        }
        // input and output are the same
        else if (expr.getPath().equalsIgnoreCase(ref.getPath()) && (!exprContainsStar || exprIsFirstWildcard)) {
            if (exprContainsStar && exprHasPrefix) {
                assert exprPrefix != null;

                int k = 0;
                result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
                for (int j=0; j < incomingSchemaSize; j++) {
                    result.outputNames.add(EMPTY_STRING);  // initialize
                }

                for (final VectorWrapper<?> wrapper : incoming) {
                    final ValueVector vvIn = wrapper.getValueVector();
                    final String incomingName = vvIn.getField().getName();
                    // get the prefix of the name
                    final String[] nameComponents = incomingName.split(StarColumnHelper.PREFIX_DELIMITER, 2);
                    // if incoming valuevector does not have a prefix, ignore it since this expression is not referencing it
                    if (nameComponents.length <= 1) {
                        k++;
                        continue;
                    }
                    final String namePrefix = nameComponents[0];
                    if (exprPrefix.equalsIgnoreCase(namePrefix)) {
                        if (!result.outputMap.containsKey(incomingName)) {
                            result.outputNames.set(k, incomingName);
                            result.outputMap.put(incomingName, incomingName);
                        }
                    }
                    k++;
                }
            } else {
                result.outputNames = Lists.newArrayList();
                if (exprContainsStar) {
                    for (final VectorWrapper<?> wrapper : incoming) {
                        final ValueVector vvIn = wrapper.getValueVector();
                        final String incomingName = vvIn.getField().getName();
                        if (refContainsStar) {
                            addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
                        } else {
                            addToResultMaps(incomingName, result, false);
                        }
                    }
                } else {
                    final String newName = expr.getPath();
                    if (!refHasPrefix && !exprHasPrefix) {
                        addToResultMaps(newName, result, true); // allow dups since this is likely top-level project
                    } else {
                        addToResultMaps(newName, result, false);
                    }
                }
            }
        }

        // input is wildcard and it is not the first wildcard
        else if (exprIsStar) {
            result.outputNames = Lists.newArrayList();
            for (final VectorWrapper<?> wrapper : incoming) {
                final ValueVector vvIn = wrapper.getValueVector();
                final String incomingName = vvIn.getField().getName();
                addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
            }
        }

        // only the output has prefix
        else if (!exprHasPrefix && refHasPrefix) {
            result.outputNames = Lists.newArrayList();
            final String newName = ref.getPath();
            addToResultMaps(newName, result, false);
        }
        // input has prefix but output does not
        else if (exprHasPrefix && !refHasPrefix) {
            int k = 0;
            result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
            for (int j=0; j < incomingSchemaSize; j++) {
                result.outputNames.add(EMPTY_STRING);  // initialize
            }

            for (final VectorWrapper<?> wrapper : incoming) {
                final ValueVector vvIn = wrapper.getValueVector();
                final String name = vvIn.getField().getName();
                final String[] components = name.split(StarColumnHelper.PREFIX_DELIMITER, 2);
                if (components.length <= 1)  {
                    k++;
                    continue;
                }
                final String namePrefix = components[0];
                final String nameSuffix = components[1];
                if (exprPrefix.equalsIgnoreCase(namePrefix)) {  // // case insensitive matching of prefix.
                    if (refContainsStar) {
                        // remove the prefix from the incoming column names
                        final String newName = getUniqueName(nameSuffix, result);  // for top level we need to make names unique
                        result.outputNames.set(k, newName);
                    } else if (exprSuffix.equalsIgnoreCase(nameSuffix)) { // case insensitive matching of field name.
                        // example: ref: $f1, expr: T0<PREFIX><column_name>
                        final String newName = ref.getPath();
                        result.outputNames.set(k, newName);
                    }
                } else {
                    result.outputNames.add(EMPTY_STRING);
                }
                k++;
            }
        }
        // input and output have prefixes although they could be different...
        else if (exprHasPrefix && refHasPrefix) {
            final String[] input = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
            assert(input.length == 2);
            assert false : "Unexpected project expression or reference";  // not handled yet
        }
        else {
            // if the incoming schema's column name matches the expression name of the Project,
            // then we just want to pick the ref name as the output column name

            result.outputNames = Lists.newArrayList();
            for (final VectorWrapper<?> wrapper : incoming) {
                final ValueVector vvIn = wrapper.getValueVector();
                final String incomingName = vvIn.getField().getName();
                if (expr.getPath().equalsIgnoreCase(incomingName)) {  // case insensitive matching of field name.
                    final String newName = ref.getPath();
                    addToResultMaps(newName, result, true);
                }
            }
        }
    }


}
