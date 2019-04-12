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

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

public class HandlerReadThread implements Runnable
{
    private BlockingQueue inqueue;
    private PriorityQueue<InQueueObject> pq;
    private List<Integer> serverTimeStamp = null;
    List<Integer> mutationTimeStamp;


    public HandlerReadThread(BlockingQueue inqueue) {
        Comparator<InQueueObject> comparator= new Comparator<InQueueObject>() {
            @Override
            public int compare(InQueueObject o1, InQueueObject o2) {
                List<Integer> t1_vector = o1.getMutationTimeStamp();
                List<Integer> t2_vector = o2.getMutationTimeStamp();
                for (int index = 0; index < t1_vector.size(); index++) {
                    int t1 = t1_vector.get(index);
                    int t2 = t2_vector.get(index);
                    if (t1 < t2) {
                        return -1;
                    } else if (t1 > t2) {
                        return 1;
                    }
                }
                return 0;
            }
        };
        this.inqueue = inqueue;
        pq = new PriorityQueue<>(comparator);
    }

    // Check whether we can directly commit this mutation
    private boolean canCommit(List<Integer> serverTimeStamp, List<Integer> mutationTimeStamp, int senderID) {
        for (int i = 0; i < serverTimeStamp.size(); i++) {
            if (i == senderID) {
                if (mutationTimeStamp.get(i) != serverTimeStamp.get(i) + 1) {
                    return false;
                }
            } else {
                if (mutationTimeStamp.get(i) > serverTimeStamp.get(i)) {
                    return false;
                }
            }
        }
        // preventing rereading from the disk to fetch the local timeStamp;
        serverTimeStamp.set(senderID, serverTimeStamp.get(senderID) + 1);
        return true;
    }

    private void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    private void commit(MessageIn<Mutation> message, int id, InetAddressAndPort replyTo) {
        message.payload.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
            failed();
            return null;
        });
    }

    private void batchCommit() {
        while (!pq.isEmpty()) {
            InQueueObject newMutation = (InQueueObject) inqueue.poll();
            mutationTimeStamp = newMutation.getMutationTimeStamp();
            int senderID = newMutation.getSenderID();
            if (canCommit(serverTimeStamp, mutationTimeStamp, senderID)) {
                commit(newMutation.getMessage(), newMutation.getId(), newMutation.getReplyTo());
            } else {
                return;
            }
        }
    }

    @Override
    public void run(){
        while (true) {
            while (!inqueue.isEmpty()) {
                InQueueObject newMutation = (InQueueObject) inqueue.poll();
                // fetch the mutation timeStamp and the timeStamp in this server
                // into the InQueue Object
                newMutation.fetchTimeStamp();

                //Get the current mutationTimeStamp
                mutationTimeStamp = newMutation.getMutationTimeStamp();

               // need to read ServerTimeStamp here.

                // Get the mutation Sender id
                int mutationSenderID = newMutation.getSenderID();
                if (canCommit(serverTimeStamp, mutationTimeStamp, mutationSenderID)) {
                    commit(newMutation.getMessage(), newMutation.getId(), newMutation.getReplyTo());
                    //After this Mutation we can do a batch
                    batchCommit();

                } else {
                    //offer the newMutation into the priorityQueue
                    // Donot need to do anything since in this case there will not going to be any valid mutation
                    pq.offer(newMutation);
                }
            }
        }
    }
}
