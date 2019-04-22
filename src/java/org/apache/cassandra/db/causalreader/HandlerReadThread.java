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

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class HandlerReadThread extends Thread
{
    private BlockingQueue inqueue;
    private PriorityQueue<InQueueObject> pq;
    Integer mutationSenderID;
    private List<Integer> serverTimeStamp = null;
    List<Integer> mutationTimeStamp;
    private static final Logger logger = LoggerFactory.getLogger(HandlerReadThread.class);
//    private Condition conv;
//    private Lock lock;


    public HandlerReadThread(BlockingQueue inqueue)
    {
        Comparator<InQueueObject> comparator = new Comparator<InQueueObject>()
        {
            @Override
            public int compare(InQueueObject o1, InQueueObject o2)
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
//                    logger.debug(t1_entry + " : " + t2_entry);

                    if (t1_entry > t2_entry) {
                        cond1 = false;
                    } else if (t1_entry < t2_entry) {
                        cond2 = false;
                    }
                }

                long physical_t1 = o1.getPhysicalTimeStamp();
                long physical_t2 = o2.getPhysicalTimeStamp();
                if (cond1 == cond2) {
//                    logger.debug("Same Condition" + cond1 + " " + cond2);
                    return physical_t1 < physical_t2 ? -1 : 1;
                } else {
                    return cond1 ? -1 : 1;
                }

            }
        };
        System.out.println("Thread initiates");
        logger.debug("Thread initiates");
        this.inqueue = inqueue;
        pq = new PriorityQueue<>(comparator);
    }

    @Override
    public void run()
    {

        while (true)
        {
            logger.debug("Inside thread now");

            try
            {
                logger.warn("Wait here since queue is Empty");
                logger.debug("The BlockingQueue size is" + inqueue.size()+"");
                InQueueObject message = (InQueueObject) inqueue.take();
                logger.warn("Queue is not Empty now");


                // Get the mutation Sender id
//                logger.warn("print SenderId");
                mutationSenderID = message.getSenderID();
//                logger.warn("Get from senderId:" + mutationSenderID);

                //Get the current mutationTimeStamp
//                logger.warn("print mutationTime");
                mutationTimeStamp = message.getMutationTimeStamp();
//                logger.warn("Get From ServerTimeStamp: " + printList(mutationTimeStamp));

                // need to read ServerTimeStamp here.
//                logger.warn("print LocalTime");
                serverTimeStamp = CausalCommon.getInstance().fetchLocalTimeStamp(message.getMessage());
//                logger.warn("Get From localTime: " + printList(serverTimeStamp));

                if (CausalCommon.getInstance().canCommit(serverTimeStamp, mutationTimeStamp, mutationSenderID))
                {
                    // Need to create our own mutation since we donot want to commit the mutation by others
                    // just need to commit value and increment one corresponding entry by one;
//                    logger.warn("Can Commit");
//                    Mutation commitMutation = CausalCommon.getInstance().createCommitMutation(message, serverTimeStamp);
//                    CausalCommon.getInstance().commit(commitMutation, message.getId(), message.getReplyTo());
                    //After this Mutation we can do a batch
                    batchCommit();
                }
                else
                {
                    logger.warn("Cannot Commit");
                    //offer the message into the priorityQueue
                    // Donot need to do anything since in this case there will not going to be any valid mutation
                    pq.offer(message);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    //Commit the leftover Object inside the Queue
    private void batchCommit()
    {
//        logger.warn("Doing batch commit");
        while (!pq.isEmpty())
        {
//            logger.warn("Not Empty");
            InQueueObject newMessage = (InQueueObject) inqueue.poll();

            mutationTimeStamp = newMessage.getMutationTimeStamp();
            // Get the mutation Sender id
            mutationSenderID = newMessage.getSenderID();
            // need to read ServerTimeStamp here.
            serverTimeStamp = CausalCommon.getInstance().fetchLocalTimeStamp(newMessage.getMessage());

//            logger.warn("MutationTimeStamp" + printList(mutationTimeStamp));
//            logger.warn("MutationSenderID" + mutationSenderID);
//            logger.warn("ServerTimeStamp" + printList(serverTimeStamp));


            if (CausalCommon.getInstance().canCommit(serverTimeStamp, mutationTimeStamp, mutationSenderID))
            {
                //Build our own Mutation
//                Mutation commitMutation = CausalCommon.getInstance().createCommitMutation(newMessage, serverTimeStamp);
//                CausalCommon.getInstance().commit(commitMutation, newMessage.getId(), newMessage.getReplyTo());
            }
            else
            {
//                logger.warn("End batch.");
                return;
            }
        }
    }

    private String printList(List<Integer> l)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < l.size(); i++)
        {
//            logger.debug(l.get(i) + "");
            sb.append(l.get(i));
        }
        return sb.toString();
    }
}