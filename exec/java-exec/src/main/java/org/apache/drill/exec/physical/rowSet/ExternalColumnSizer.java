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
 ******************************************************************************/

package org.apache.drill.exec.physical.rowSet;

/**
 * Sizer for the ResultSetLoader to know about the size of
 * data in the columns that are part of the batch but are not
 * created by the ResultSetLoader
 *
 * For example, the size of columns that are passed unchanged by an operator
 * by setting up a tranfer-pair
 * */
public interface ExternalColumnSizer {
    int getAverageExternalRowSize();

    //KM_TBD implement per row size
    //int getExternalRowSize();
}
