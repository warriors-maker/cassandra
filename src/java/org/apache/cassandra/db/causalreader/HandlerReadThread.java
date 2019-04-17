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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
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

public class HandlerReadThread implements Runnable
{
    private BlockingQueue inqueue;
    private PriorityQueue<InQueueObject> pq;
    Integer mutationSenderID;
    private List<Integer> serverTimeStamp = null;
    List<Integer> mutationTimeStamp;
    private Condition conv;
    private static final Logger logger = LoggerFactory.getLogger(HandlerReadThread.class);
    private Lock lock;

    public HandlerReadThread(BlockingQueue inqueue, Condition conv, Lock lock) {
        this.conv = conv;
        this.lock = lock;
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
        logger.debug("Check if we can Commit");
        logger.warn("ServerTimeStamp" + printList(serverTimeStamp));
        logger.warn("MutationTimeStamp" + printList(mutationTimeStamp));
        if (serverTimeStamp.size() == 0) {
            return true;
        }
        for (int i = 0; i < serverTimeStamp.size(); i++) {
            if (i == senderID) {
                if (mutationTimeStamp.get(i) != serverTimeStamp.get(i) + 1) {
                    logger.warn("Sender fields fail");
                    return false;
                }
            } else {
                if (mutationTimeStamp.get(i) > serverTimeStamp.get(i)) {
                    logger.warn("Other fails");
                    return false;
                }
            }
        }
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

    private void commit(Mutation commitMutation, int id, InetAddressAndPort replyTo) {
        logger.warn("Commit successfully");
        commitMutation.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
            failed();
            return null;
        });
    }

    //Commit the leftover Object inside the Queue
    private void batchCommit() {
        logger.warn("Doing batch commit");
        while (!pq.isEmpty()) {

            InQueueObject newMessage = (InQueueObject) inqueue.poll();

            mutationTimeStamp = newMessage.getMutationTimeStamp();
            // Get the mutation Sender id
            mutationSenderID = newMessage.getSenderID();
            // need to read ServerTimeStamp here.
            serverTimeStamp = fetchLocalTimeStamp(newMessage.getMessage());

            logger.warn("MutationTimeStamp" + printList(mutationTimeStamp));
            logger.warn("MutationSenderID" +mutationSenderID);
            logger.warn("ServerTimeStamp" + printList(serverTimeStamp));


            if (canCommit(serverTimeStamp, mutationTimeStamp, mutationSenderID)) {
                //Build our own Mutation
                Mutation commitMutation = createCommitMutation(newMessage);
                commit(commitMutation, newMessage.getId(), newMessage.getReplyTo());
            }
            else {
                logger.warn("End batch.");
                return;
            }
        }
    }

    // TODO: Need to carefully check this part
    private Mutation createCommitMutation(InQueueObject message) {
        logger.warn("Create Our Mutation");
        Mutation incomingMutation = message.getMutation();
        Row mutationRow = incomingMutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(incomingMutation.getKeyspaceName(), incomingMutation.key());


        TableMetadata tableMetadata = incomingMutation.getPartitionUpdates().iterator().next().metadata();
        int senderID = message.getSenderID();
        for (Cell c : mutationRow.cells())
        {
            String colName = c.column().name.toString();
            // Since the metaData in the mutation may not exist in our table

            // We only want the value and the corresponding timeStamp;
            if (colName.startsWith("vcol"+senderID)) {
                int value = ByteBufferUtil.toInt(c.value());
                logger.warn("The corresponding time col and time value is "+ senderID + " : " + value);
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            } else if (colName.startsWith("vcol")) {
                if (c == null || c.value() == null) {
                    mutationBuilder.update(tableMetadata).row().add(colName, 0);
                }
            }
            // if the value is a integer type
            else if (IntegerType.instance.isValueCompatibleWithInternal(c.column().cellValueType())) {
                int value = ByteBufferUtil.toInt(c.value());
                logger.warn("The new value is " + value);
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }
            // if it is a string type
            else if (UTF8Type.instance.isValueCompatibleWith(c.column().cellValueType())) {
                String value = "";
                try {
                    value = ByteBufferUtil.string(c.value());
                } catch (CharacterCodingException e) {
                    e.printStackTrace();
                }
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }

        }
        List<Integer> mutationTimeStamp = message.getMutationTimeStamp();
        mutationBuilder.update(tableMetadata).row().add(CausalUtility.getColPrefix() + senderID,mutationTimeStamp.get(senderID));
        return mutationBuilder.build();
    }

    private List<Integer> fetchLocalTimeStamp(MessageIn<Mutation> message) {
        try
        {
            // first we have to create a read request out of the current mutation
            // to read out the currentTimeStamp from locally
            SinglePartitionReadCommand localRead =
            SinglePartitionReadCommand.fullPartitionRead(
            message.payload.getPartitionUpdates().iterator().next().metadata(),
            FBUtilities.nowInSeconds(),
            message.payload.key()
            );

            List<Integer> localTimeStamp = new ArrayList<>();
            //Extract tag information from the local read into a Vector (List) timeStamp
            try (ReadExecutionController executionController = localRead.executionController();
                 UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController))
            {
                // first we have to transform it into a PartitionIterator
                PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());
                while (pi.hasNext())
                {
                    RowIterator ri = pi.next();
                    while (ri.hasNext())
                    {
                        Row localRow = ri.next();

                        //Fetch the server timeStamp
                        //Fetch the mutation TimeStamp
                        for (int server_id = 0; server_id < CausalUtility.getNumNodes(); server_id++)
                        {
                            // Read through individual column, which are the time_Vector_Entry
                            String colName = CausalUtility.getColPrefix() + server_id;
                            // reading the current Server TimeStamp;
                            ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(colName));
                            Cell c = localRow.getCell(colMeta);
                            int local_vector_col_time = ByteBufferUtil.toInt(c.value());
                            logger.debug("Get my LocalTime" + colName + " " + ByteBufferUtil.toInt(c.value()));
                            // Whenever there is a mutation on current server, update its corresponding timeStamp
                            localTimeStamp.add(local_vector_col_time);
                        }
                    }
                }
                // Since in our case we only need to consider one Row
                if (localTimeStamp.size() == 0) {
                    logger.debug("LocalTimeStamp is 0");
                }
                return localTimeStamp;
            }
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
        return null;
    }

    @Override
    public void run(){

        while (true) {
            logger.debug("Inside thread now");
            while (!inqueue.isEmpty()) {

                InQueueObject message = (InQueueObject) inqueue.poll();

                message.initOtherFields();

                // Get the mutation Sender id
                logger.warn("print SenderId");
                mutationSenderID = message.getSenderID();
                logger.warn("Get from senderId:" + mutationSenderID );

                //Get the current mutationTimeStamp
                logger.warn("print mutationTime");
                mutationTimeStamp = message.getMutationTimeStamp();
                logger.warn("Get From ServerTimeStamp: " + printList(mutationTimeStamp));

                // need to read ServerTimeStamp here.

                logger.warn("print LocalTime");
                serverTimeStamp = fetchLocalTimeStamp(message.getMessage());
                logger.warn("Get From localTime: " + printList(serverTimeStamp));


                if (canCommit(serverTimeStamp, mutationTimeStamp, mutationSenderID)) {
                    // Need to create our own mutation since we donot want to commit the mutation by others
                    // just need to commit value and increment one corresponding entry by one;
                    logger.warn("Can Commit");
                    Mutation commitMutation = createCommitMutation(message);
                    commit(commitMutation, message.getId(), message.getReplyTo());
                    //After this Mutation we can do a batch
                    batchCommit();

                } else {
                    logger.warn("Cannot Commit");
                    //offer the message into the priorityQueue
                    // Donot need to do anything since in this case there will not going to be any valid mutation
                    pq.offer(message);
                }
            }
            try{
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }

//            synchronized (lock) {
//                try
//                {
//                    logger.warn("Start to wait");
//                    conv.wait();
//                    logger.warn("Being awoken by main thread");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
        }
    }

    private String printList(List<Integer> l) {
        logger.debug("Printing list");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < l.size(); i++) {
            logger.debug(l.get(i) + "");
            sb.append(l.get(i));
        }
        return sb.toString();
    }
}
