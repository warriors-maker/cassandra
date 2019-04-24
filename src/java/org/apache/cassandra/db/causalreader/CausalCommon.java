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
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.service.StorageProxy.sendToHintedEndpoints;

public class CausalCommon
{
    private static final Logger logger = LoggerFactory.getLogger(CausalCommon.class);
    private static CausalCommon causalCommon= new CausalCommon();

    private CausalCommon() {

    }

    public static CausalCommon getInstance() {
        return causalCommon;
    }

    // Only need the value
    public Mutation createCommitMutation(Mutation incomingMutation)
    {
//        logger.warn("Create Our Mutation");
//        printMutation(incomingMutation);

        Row mutationRow = incomingMutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(incomingMutation.getKeyspaceName(), incomingMutation.key());


        TableMetadata tableMetadata = incomingMutation.getPartitionUpdates().iterator().next().metadata();
        for (Cell c : mutationRow.cells())
        {
            String colName = c.column().name.toString();
            // Since the metaData in the mutation may not exist in our table
//            logger.debug(colName);
            // We only want the value and the corresponding timeStamp;
            if (colName.startsWith("vcol") || colName.startsWith("sendfrom"))
            {
                continue;
            }
            // if the value is a integer type
            else if (IntegerType.instance.isValueCompatibleWithInternal(c.column().cellValueType()))
            {
                int value = ByteBufferUtil.toInt(c.value());
//                logger.warn("The new value is " + value);
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }
            // if it is a string type
            else if (UTF8Type.instance.isValueCompatibleWith(c.column().cellValueType()))
            {
                String value = "";
                try
                {
                    value = ByteBufferUtil.string(c.value());
//                    logger.warn("The new value is " + value);
                }
                catch (CharacterCodingException e)
                {
                    e.printStackTrace();
                }
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }
        }

        return mutationBuilder.build();
    }


    public void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }


    // Check whether we can directly commit this mutation
    public boolean canCommit(List<Integer> serverTimeStamp, List<Integer> mutationTimeStamp, int senderID)
    {
//        logger.debug("Check if we can Commit");
//        logger.warn("ServerTimeStamp" + printList(serverTimeStamp));
//        logger.warn("MutationTimeStamp" + printList(mutationTimeStamp));
        // Maynot need this
        for (int i = 0; i < serverTimeStamp.size(); i++)
        {
            if (i == senderID)
            {
                if (mutationTimeStamp.get(i) != serverTimeStamp.get(i) + 1)
                {
//                    logger.warn("Sender fields fail");
                    return false;
                }
            }
            else
            {
                if (mutationTimeStamp.get(i) > serverTimeStamp.get(i))
                {
//                    logger.warn("Other fails");
                    return false;
                }
            }
        }
        return true;
    }

    public void commit(Mutation commitMutation)
    {
        commitMutation.apply();
    }

    public int getSenderID(Mutation mutation)
    {
        Row mutationRow = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        int senderID = 0;
        for (Cell c : mutationRow.cells())
        {
            // fetch the individual timeEntry
//            logger.debug("Column name is: " + c.column().name.toString());
            String colName = c.column().name.toString();
            // fetch the SenderCol Name
            if (c.column().name.equals(new ColumnIdentifier(CausalUtility.getSenderColName(), true)))
            {
//                logger.warn("The sender is " + ByteBufferUtil.toInt(c.value()));
                senderID =  ByteBufferUtil.toInt(c.value());
            }
        }
        return senderID;
    }

    public List<Integer> getMutationTimeStamp(Mutation mutation) {
        List<Integer> mutationTimeStamp = new ArrayList<>();
        Row mutationRow = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        // TODO: Need to check whether it is in the order we define the schema
        for (Cell c : mutationRow.cells()) {
            // fetch the individual timeEntry
//            logger.debug("Column name is: " + c.column().name.toString());
            String colName = c.column().name.toString();
            if (colName.startsWith(CausalUtility.getColPrefix())) {
//                logger.warn(c.column().name.toString() + ByteBufferUtil.toInt(c.value()));
                mutationTimeStamp.add(ByteBufferUtil.toInt(c.value()));
            }
        }
        return mutationTimeStamp;
    }



    public boolean isDataMutation(IMutation mutation) {
        //check if the Mutation (our time) only needs to perform locally by checking if it has the sendFrom column
        Row mutationRow = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        for (Cell c : mutationRow.cells())
        {
            String colName = c.column().name.toString();
            if (colName.startsWith("field"))
            {
                return true;
            }
        }
        return false;
    }


    public void printFailMutation(PQObject pqObject) {
        if (pqObject == null) {
            logger.debug("Null");
        } else {
            logger.debug("We Cannot commit directly time: " + CausalCommon.getInstance().getTimeStamp(pqObject.getMutationTimeStamp()));
        }
    }

    public void printHeadMutation(PQObject pqObject) {
        if (pqObject != null) {
            logger.debug("Head in PQ: " + CausalCommon.getInstance().getTimeStamp(pqObject.getMutationTimeStamp()));
        }
    }

    public void printLocalTimeStamp(List<Integer> timeStamp) {
        StringBuilder sb = new StringBuilder();
        sb.append("Local time is ");
        for (int i = 0; i < timeStamp.size(); i++) {
            sb.append (timeStamp.get(i) + ",");
        }
        logger.debug(sb.toString());
    }

    public void printMutateTimeStamp(List<Integer> timeStamp) {
        StringBuilder sb = new StringBuilder();
        sb.append("Mutate time is ");
        for (int i = 0; i < timeStamp.size(); i++) {
            sb.append (timeStamp.get(i) + ",");
        }
        logger.debug(sb.toString());
    }

    public String getTimeStamp(List<Integer> timeStamp) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < timeStamp.size(); i++) {
            sb.append (timeStamp.get(i) + ",");
        }
        return sb.toString();
    }

    public void printPQ(PriorityBlockingQueue<PQObject> pq) {
        PriorityBlockingQueue<PQObject> copy = new PriorityBlockingQueue(pq);

        StringBuilder sb = new StringBuilder();
        sb.append("MyPQ value:");
        synchronized (pq) {
            int size = copy.size();
            for (int i = 0; i <size; i++) {
                PQObject pqObject = copy.poll();
                List<Integer> time = pqObject.getMutationTimeStamp();
                for (int j = 0; j < 3; j++) {
                    sb.append(time.get(j) + ",");
                }
                sb.append("\n");
            }
        }
        logger.debug(sb.toString());
    }




}