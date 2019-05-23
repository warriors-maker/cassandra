/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.Treas;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConfig
{
    public final static int num_server = 5;
    public final static int num_intersect = 5;
    public final static int num_recover = 2;
    public final static int num_concurrecy = 3;

    public static final String TAG_ONE  = "tag1";
    public static final String TAG_TWO  = "tag2";
    public static final String TAG_THREE  = "tag3";

    public static final String VAL_ONE =  "field1";
    public static final String VAL_TWO =  "field2";
    public static final String VAL_THREE =  "field3";

    public static final String VAL_PREFIX =  "field";
    public static final String TAG_PREFIX =  "tag";

    public static  ColumnIdentifier tagOneIdentifier = new ColumnIdentifier(TAG_ONE, true);
    public static  ColumnIdentifier tagTwoIdentifier = new ColumnIdentifier(TAG_TWO, true);
    public static  ColumnIdentifier tagThreeIdentifier = new ColumnIdentifier(TAG_THREE, true);
    public static  ColumnIdentifier valOneIdentifier = new ColumnIdentifier(VAL_ONE, true);
    public static  ColumnIdentifier valTwoIdentifier = new ColumnIdentifier(VAL_TWO, true);
    public static  ColumnIdentifier valThreeIdentifier = new ColumnIdentifier(VAL_THREE, true);
}
