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
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.causalreader.CausalCommon;
import org.apache.cassandra.db.causalreader.CausalObject;
import org.apache.cassandra.db.causalreader.CausalUtility;
import org.apache.cassandra.db.causalreader.PQObject;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    private CausalObject causalObject;
    private static final Logger logger = LoggerFactory.getLogger(MutationVerbHandler.class);

    private void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    public MutationVerbHandler() {

    }

    public MutationVerbHandler(CausalObject causalObject) {
        this.causalObject = causalObject;
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException {
        // Check if there were any forwarding headers in this message
        logger.debug("Doverb");
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

        logger.debug("Fetch Value");
        Mutation mutation = message.payload;
        logger.debug("Mutation key is" + mutation.key().toString());
        TableMetadata timeVectorMeta = Keyspace.open(mutation.getKeyspaceName()).getMetadata().getTableOrViewNullable("server");
        DecoratedKey myKey = timeVectorMeta.partitioner.decorateKey(ByteBuffer.wrap(Integer.toString(CausalUtility.getWriterID()).getBytes()));

        //Check whether we have inititate our timeStamp already
        if (!CausalCommon.getInstance().isVectorInitiate(timeVectorMeta)) {
            logger.debug("Have not Initiated");
            CausalCommon.getInstance().initiateTimeVector(timeVectorMeta, mutation,myKey);
        }

        //Fetch localTimeStamp
        List<Integer> localTimeVector = CausalCommon.getInstance().fetchMyTimeStamp(timeVectorMeta);
        logger.debug("Doverb LocalTimeVector:");
        CausalCommon.getInstance().printTimeStamp(localTimeVector);

        //fetch Mutation Vector
        List<Integer> mutationVector = CausalCommon.getInstance().getMutationTimeStamp(mutation);
        logger.debug("Doverb MutationTimeVector:");
        if (mutationVector.size() == 0) {
            logger.debug("Size is 0");
        }
        CausalCommon.getInstance().printTimeStamp(mutationVector);

        //Check who is the sender
        int senderID = CausalCommon.getInstance().getSenderID(mutation);

        //Compare two vectors
        //if can commit, build a new Mutation
        //if cannot commit, push them into pq;
        if (CausalCommon.getInstance().canCommit(localTimeVector, mutationVector, senderID)) {
            logger.debug("Yes, we can commit");
            //Commit the new TimeStamp
            localTimeVector.set(senderID, localTimeVector.get(senderID) + 1);
            CausalCommon.getInstance().updateLocalTimeStamp(localTimeVector, timeVectorMeta,mutation, myKey);

            // Create the new Mutation to be applied;
            Mutation newMutation = CausalCommon.getInstance().createCommitMutation(mutation,mutationVector, senderID);
            //Apply the New Mutation;
            CausalCommon.getInstance().commit(newMutation, id, replyTo);

//            // push our new Read TimeStamp into Blocking Queue;
//            if (causalObject.gerPriorityBlockingQueue().size() != 0) {
//                this.causalObject.getBlockingQueue().offer(localTimeVector);
//            }

        } else {
            logger.debug("We Cannot commit");
            // push it into our PQ
            PQObject obj = new PQObject(mutationVector, System.nanoTime(), mutation, senderID, id, replyTo);
            this.causalObject.gerPriorityBlockingQueue().offer(obj);
        }

        //Once either commit or put into the pq, we reply
        reply(id, replyTo);
    }


    private static void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, ForwardToContainer forwardTo, InetAddressAndPort from) throws IOException
    {
        // tell the recipients who to send their ack to
        MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer).withParameter(ParameterType.FORWARD_FROM, from);
        Iterator<InetAddressAndPort> iterator = forwardTo.targets.iterator();
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < forwardTo.targets.size(); i++)
        {
            InetAddressAndPort address = iterator.next();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, forwardTo.messageIds[i], address);
        }
    }
}
