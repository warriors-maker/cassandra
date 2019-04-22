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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;

public class HandlerReadThread extends Thread
{
    private BlockingQueue blockingQueue;
    private PriorityBlockingQueue<PQObject> priorityBlockingQueue;

    public HandlerReadThread(CausalObject causalObject) {
        this.blockingQueue = causalObject.getBlockingQueue();
        this.priorityBlockingQueue = causalObject.gerPriorityBlockingQueue();
    }

    private void batchCommit(PQObject pqObject) {
        // Fetch the head timeStamp
        Mutation mutation = pqObject.getMutation();

        // Fetch the current timeStamp;
        TableMetadata timeVectorMeta = Keyspace.open(mutation.getKeyspaceName()).getMetadata().getTableOrViewNullable("server");
        List<Integer> localTimeVector = CausalCommon.getInstance().fetchMyTimeStamp(timeVectorMeta);
        boolean flag = false;

        while (CausalCommon.getInstance().canCommit(localTimeVector, pqObject.getMutationTimeStamp(), pqObject.getSenderID())) {
            if (!flag) {
                flag = true;
            }
            // if can commit, poll them out
            pqObject = priorityBlockingQueue.poll();

            mutation = pqObject.getMutation();
            DecoratedKey myKey = timeVectorMeta.partitioner.decorateKey(ByteBuffer.wrap(Integer.toString(CausalUtility.getWriterID()).getBytes()));
            int senderID = pqObject.getSenderID();
            int id = pqObject.getId();
            InetAddressAndPort replyTo = pqObject.getReplyto();

            // Change out timeStamp
            localTimeVector.set(senderID, localTimeVector.get(senderID) + 1);
            CausalCommon.getInstance().updateLocalTimeStamp(localTimeVector, timeVectorMeta, mutation, myKey);

            // Create the new Mutation to be applied;
            Mutation newMutation = CausalCommon.getInstance().createCommitMutation(mutation,pqObject.getMutationTimeStamp(), senderID);
            
            //Apply the New Mutation;
            CausalCommon.getInstance().commit(newMutation, id, replyTo);

            //fetch my new TimeVector
            localTimeVector = CausalCommon.getInstance().fetchMyTimeStamp(timeVectorMeta);

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
