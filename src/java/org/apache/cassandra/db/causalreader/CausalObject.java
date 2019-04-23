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

import java.sql.Time;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

// This will be put in the Thread of the reader
public class CausalObject
{
    private BlockingQueue<List<Integer>> bq;
    private PriorityBlockingQueue pq;
    private TimeVector timeVector;

    public CausalObject (TimeVector timeVector) {
        Comparator<PQObject> comparator = new Comparator<PQObject>()
        {
            @Override
            public int compare(PQObject o1, PQObject o2)
            {
                List<Integer> t1_vector = o1.getMutationTimeStamp();
                List<Integer> t2_vector = o2.getMutationTimeStamp();
                int t1_entry;
                int t2_entry;
                boolean cond1 = true;
                boolean cond2 = true;
                for (int index = 0; index < t1_vector.size(); index++)
                {
                    t1_entry = t1_vector.get(index);
                    t2_entry = t2_vector.get(index);

                    if (t1_entry > t2_entry) {
                        cond1 = false;
                    } else if (t1_entry < t2_entry) {
                        cond2 = false;
                    }
                }
                long physical_t1 = o1.getPhysicalTimeStamp();
                long physical_t2 = o2.getPhysicalTimeStamp();
                if (cond1 == cond2) {
                    return physical_t1 < physical_t2 ? -1 : 1;
                } else {
                    return cond1 ? -1 : 1;
                }
            }
        };

        this.bq = new LinkedBlockingQueue<>();
        this.pq = new PriorityBlockingQueue<>(200000, comparator);
        this.timeVector = timeVector;
    }


    public BlockingQueue getBlockingQueue()
    {
        return bq;
    }

    public PriorityBlockingQueue getPriorityBlockingQueue()
    {
        return pq;
    }

    public TimeVector getTimeVector() {
        return this.timeVector;
    }
}
