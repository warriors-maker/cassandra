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
import java.sql.Time;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;

public class HandlerReadThread extends Thread
{
    private BlockingQueue blockingQueue;
    private PriorityBlockingQueue<PQObject> priorityBlockingQueue;
    private TimeVector timeVector;
    private static final Logger logger = LoggerFactory.getLogger(HandlerReadThread.class);

    public HandlerReadThread(CausalObject causalObject) {
        this.blockingQueue = causalObject.getBlockingQueue();
        this.priorityBlockingQueue = causalObject.gerPriorityBlockingQueue();
        this.timeVector = causalObject.getTimeVector();
    }

    private void batchCommit(PQObject pqObject) {
        // Fetch the head timeStamp
        Mutation mutation = pqObject.getMutation();

        // Fetch the current timeStamp;
        TableMetadata timeVectorMeta = Keyspace.open(mutation.getKeyspaceName()).getMetadata().getTableOrViewNullable("server");
        List<Integer> localTimeVector = timeVector.read();
        boolean flag = false;

        while (CausalCommon.getInstance().canCommit(localTimeVector, pqObject.getMutationTimeStamp(), pqObject.getSenderID())) {
            if (!flag) {
                flag = true;
            }
            // if can commit, poll them out
            pqObject = priorityBlockingQueue.poll();

            mutation = pqObject.getMutation();

            int senderID = pqObject.getSenderID();

            List<Integer> commitTime = timeVector.updateAndRead(senderID);

            logger.debug("Batch commit Time is");
            CausalCommon.getInstance().printTimeStamp(commitTime);

            // Create the new Mutation to be applied;
            Mutation newMutation = CausalCommon.getInstance().createCommitMutation(mutation);
            
            //Apply the New Mutation;
            CausalCommon.getInstance().commit(newMutation);

            //fetch my new TimeVector
            localTimeVector = timeVector.read();

            if (priorityBlockingQueue.size() == 0) {
                break;
            } else {
                pqObject = priorityBlockingQueue.peek();
            }
        }

        if (!flag) {
            priorityBlockingQueue.offer(pqObject);
        }
    }

    @Override
    public void run()
    {
        while (true) {
            try {
                PQObject object = priorityBlockingQueue.take();

                batchCommit(object);

                Thread.sleep(2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
