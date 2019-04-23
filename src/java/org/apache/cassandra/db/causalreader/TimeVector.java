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

import java.util.ArrayList;
import java.util.List;

public class TimeVector
{
    List<Integer> timeVector = new ArrayList<>();

    public TimeVector () {
        for (int i = 0; i <CausalUtility.getNumNodes(); i++) {
            timeVector.add(0);
        }
    }

    public synchronized List<Integer> updateAndRead(int id) {
        timeVector.set(id, timeVector.get(id) + 1);
        return new ArrayList<>(this.timeVector);
    }

    public synchronized List<Integer> read() {
        return new ArrayList<>(this.timeVector);
    }

}
