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

package org.apache.cassandra.db.causalreader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.cassandra.db.DecoratedKey;

public class CausalDataStructure
{
    private BlockingQueue bq;
    private PriorityBlockingQueue pq;
    private HashSet<DecoratedKey> keySet;
    private static CausalDataStructure cd = new CausalDataStructure();

    private CausalDataStructure() {
        cd.bq = new LinkedBlockingQueue();
        cd.pq = new PriorityBlockingQueue();
        cd.keySet = new HashSet<>();
    }

    public static CausalDataStructure getInstance() {
        return cd;
    }

    public BlockingQueue getBq() {
        return cd.bq;
    }

    public PriorityBlockingQueue getPq() {
        return cd.pq;
    }

    public HashSet<DecoratedKey> getKeySet() {
        return cd.keySet;
    }


}
