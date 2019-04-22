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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.causalreader.CausalCommon;
import org.apache.cassandra.db.causalreader.CausalDataStructure;
import org.apache.cassandra.db.causalreader.CausalUtility;
import org.apache.cassandra.db.causalreader.InQueueObject;
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
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.causalreader.HandlerReadThread;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    // Act like a MessageQueue in our program
    private CausalDataStructure cd;
    private static final Logger logger = LoggerFactory.getLogger(MutationVerbHandler.class);


//    private Lock aLock = new ReentrantLock();
//    private Condition condVar = aLock.newCondition();

    public MutationVerbHandler() {

    }
    public MutationVerbHandler(CausalDataStructure cd) {
        // The size is Integer.Max by Default;
        this.cd = cd;
    }

    // Replica handles Mutation from the other nodes
    public void doVerb(MessageIn<Mutation> message, int id) throws IOException {
//        logger.debug("Doverb1");
        InetAddressAndPort from = (InetAddressAndPort) message.parameters.get(ParameterType.FORWARD_FROM);
        InetAddressAndPort replyTo;

        if (from == null)
        {
//            logger.debug("FROM IS NULL");
            replyTo = message.from;
            ForwardToContainer forwardTo = (ForwardToContainer)message.parameters.get(ParameterType.FORWARD_TO);
            if (forwardTo != null)
            {
//                logger.debug("Forward to other local nodes!");
                forwardToLocalNodes(message.payload, message.verb, forwardTo, message.from);
            }
        }
        else
        {
            replyTo = from;
        }


        logger.debug("Receive Mutation");
        printMutation(message.payload);

        //fetch the primaryKey
        DecoratedKey key = message.payload.key();

        //fetch mutationTimeStamp
        List<Integer> mutationTimeStamp = CausalCommon.getInstance().getMutationTimeStamp(message);

        //read my localTimeStamp
        List<Integer> serverTimeStamp = CausalCommon.getInstance().fetchLocalTimeStamp(message);

        //Reader the senderID
        int senderID = CausalCommon.getInstance().getSenderID(message);


        //check if can commit
        if (CausalCommon.getInstance().canCommit(mutationTimeStamp,serverTimeStamp,senderID)) {
            if (cd.getKeySet().contains(key)) {

            } else {
                Mutation commitMutation = CausalCommon.getInstance().createCommitMutation(message.payload,serverTimeStamp, mutationTimeStamp, senderID);
                CausalCommon.getInstance().commit(commitMutation,id, replyTo);
            }
        } else {
            InQueueObject newMessage = new InQueueObject(message, id, replyTo, System.nanoTime());
//            pq.offer(newMessage);
        }

    }

    private void printMutation(Mutation mutation) {
        Row mutationRow = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        logger.warn("Printing the individual column of income message");
        for (Cell c : mutationRow.cells()) {
            String colName = c.column().name.toString();
            logger.debug(colName);

           if (IntegerType.instance.isValueCompatibleWithInternal(c.column().cellValueType()))
            {
                int value = ByteBufferUtil.toInt(c.value());
                logger.warn("The value is " + value);

            }
            // if it is a string type
            else if (UTF8Type.instance.isValueCompatibleWith(c.column().cellValueType()))
            {
                String value = "";
                try
                {
                    value = ByteBufferUtil.string(c.value());
                    logger.warn("The value is" + value);
                }
                catch (CharacterCodingException e)
                {
                    e.printStackTrace();
                }
            }

        }
        logger.debug("Finish Initiate");
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


    public void doVerbABD(MessageIn<Mutation> message, int id)  throws IOException
    {
        // Check if there were any forwarding headers in this message
        InetAddressAndPort from = (InetAddressAndPort)message.parameters.get(ParameterType.FORWARD_FROM);
        InetAddressAndPort replyTo;
        if (from == null)
        {
            replyTo = message.from;
            ForwardToContainer forwardTo = (ForwardToContainer)message.parameters.get(ParameterType.FORWARD_TO);
            if (forwardTo != null)
                forwardToLocalNodes(message.payload, message.verb, forwardTo, message.from);
        }
        else
        {
            replyTo = from;
        }

        try
        {
            // first we have to create a read request out of the current mutation
            SinglePartitionReadCommand localRead =
            SinglePartitionReadCommand.fullPartitionRead(
            message.payload.getPartitionUpdates().iterator().next().metadata(),
            FBUtilities.nowInSeconds(),
            message.payload.key()
            );

            int z_value_local = -1;
            String writer_id_local = "";

            // execute the read request locally to obtain the tag of the key
            // and extract tag information from the local read
            try (ReadExecutionController executionController = localRead.executionController();
                 UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController))
            {
                // first we have to transform it into a PartitionIterator
                PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());
                while(pi.hasNext())
                {
                    RowIterator ri = pi.next();
                    while(ri.hasNext())
                    {
                        Row r = ri.next();

                        ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes("z_value"));
                        Cell c = r.getCell(colMeta);
                        z_value_local = ByteBufferUtil.toInt(c.value());

                        colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes("writer_id"));

                        c = r.getCell(colMeta);
                        writer_id_local = ByteBufferUtil.string(c.value());
                    }
                }
            }

            // extract the tag information from the mutation
            int z_value_request = 0;
            String writer_id_request = "";

            Row data = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
            for (Cell c : data.cells())
            {
                if(c.column().name.equals(new ColumnIdentifier("z_value", true)))
                {
                    z_value_request = ByteBufferUtil.toInt(c.value());
                }
                else if(c.column().name.equals(new ColumnIdentifier("writer_id", true)))
                {
                    writer_id_request = ByteBufferUtil.string(c.value());
                }
            }

            //System.out.printf("local z:%d %s request z:%d %s\n", z_value_local, writer_id_local, z_value_request, writer_id_request);

            // comparing the tag and the one in mutation, act accordingly
            if (z_value_request > z_value_local || (z_value_request == z_value_local && writer_id_request.compareTo(writer_id_local) > 0))
            {
                // this payload is the mutation right?
                message.payload.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
                    failed();
                    return null;
                });
            }
            else
            {
                reply(id, replyTo);
            }
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    private static void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, ForwardToContainer forwardTo, InetAddressAndPort from) throws IOException
    {
//        logger.debug("Forward Start");
        // tell the recipients who to send their ack to
        MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer).withParameter(ParameterType.FORWARD_FROM, from);
        Iterator<InetAddressAndPort> iterator = forwardTo.targets.iterator();
        // Send a message to each of the addresses on our Forward List
//        logger.debug("Forward size:" + forwardTo.targets.size()+"");
        for (int i = 0; i < forwardTo.targets.size(); i++)
        {
//            logger.debug("Forward Index" + i);
            InetAddressAndPort address = iterator.next();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, forwardTo.messageIds[i], address);
        }
//        logger.debug("Forward Done");
    }

}
