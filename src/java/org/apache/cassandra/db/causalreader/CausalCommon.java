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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
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
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public class CausalCommon
{
    private static final Logger logger = LoggerFactory.getLogger(HandlerReadThread.class);
    private static CausalCommon causalCommon= new CausalCommon();

    private CausalCommon() {

    }

    public static CausalCommon getInstance() {
        return causalCommon;
    }

    // TODO: Need to carefully check this part
    public Mutation createCommitMutation(Mutation incomingMutation, List<Integer> localTimeStamp, List<Integer> mutationTimeStamp, int senderID)
    {
        logger.warn("Create Our Mutation");
        printMutation(incomingMutation);

        Row mutationRow = incomingMutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(incomingMutation.getKeyspaceName(), incomingMutation.key());


        TableMetadata tableMetadata = incomingMutation.getPartitionUpdates().iterator().next().metadata();
        for (Cell c : mutationRow.cells())
        {
            String colName = c.column().name.toString();
            // Since the metaData in the mutation may not exist in our table
//            logger.debug(colName);
            // We only want the value and the corresponding timeStamp;
            if (colName.startsWith("vcol" + senderID))
            {
                int value = ByteBufferUtil.toInt(c.value());
//                logger.warn("The corresponding time col and time value is " + senderID + " : " + value);
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }
            else if (colName.startsWith("vcol"))
            {
                int index = Integer.parseInt(colName.substring(4));
//                logger.debug("Our index is" + index +
//                             " MyLocal value is " + localTimeStamp.get(index) +
//                             " MutationLocal is " + ByteBufferUtil.toInt(c.value()));
                mutationBuilder.update(tableMetadata).row().add(colName, localTimeStamp.get(index));
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
                }
                catch (CharacterCodingException e)
                {
                    e.printStackTrace();
                }
                mutationBuilder.update(tableMetadata).row().add(colName, value);
            }
        }
        mutationBuilder.update(tableMetadata).row().add(CausalUtility.getColPrefix() + senderID, mutationTimeStamp.get(senderID));
        return mutationBuilder.build();
    }

    public List<Integer> fetchLocalTimeStamp(MessageIn<Mutation> message)
    {
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
            //Extract timestamp information from the local read into a Vector (List) timeStamp
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

                        //Fetch the mutation TimeStamp
                        for (int server_id = 0; server_id < CausalUtility.getNumNodes(); server_id++)
                        {
                            // Read through individual column, which are the time_Vector_Entry
                            String colName = CausalUtility.getColPrefix() + server_id;
                            // reading the current Server TimeStamp;
                            ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(colName));
                            Cell c = localRow.getCell(colMeta);
                            int local_vector_col_time = ByteBufferUtil.toInt(c.value());
//                            logger.debug("Get my LocalTime" + colName + " " + ByteBufferUtil.toInt(c.value()));
                            // Whenever there is a mutation on current server, update its corresponding timeStamp
                            localTimeStamp.add(local_vector_col_time);
                        }
                    }
                }
                // Since in our case we only need to consider one Row;
                // When size ==0 means that we donot have this timestamp;
                if (localTimeStamp.size() == 0)
                {
                    for (int i = 0; i < CausalUtility.getNumNodes(); i++)
                    {
                        localTimeStamp.add(0);
//                        logger.debug("NoLocalData so time is" + 0);
                    }
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

    public void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    // Check whether we can directly commit this mutation
    public boolean canCommit(List<Integer> serverTimeStamp, List<Integer> mutationTimeStamp, int senderID)
    {
//        logger.debug("Check if we can Commit");
//        logger.warn("ServerTimeStamp" + printList(serverTimeStamp));
//        logger.warn("MutationTimeStamp" + printList(mutationTimeStamp));
        // Maynot need this
        if (serverTimeStamp.size() == 0)
        {
            return true;
        }
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

    public void commit(Mutation commitMutation, int id, InetAddressAndPort replyTo)
    {
//        logger.warn("Commit successfully");
        commitMutation.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
            failed();
            return null;
        });
    }

    public int getSenderID(MessageIn<Mutation> message)
    {
        Row mutationRow = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
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

    public List<Integer> getMutationTimeStamp(MessageIn<Mutation> message) {
        List<Integer> mutationTimeStamp = new ArrayList<>();
        Row mutationRow = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
//        logger.warn("Initiate serverTimestamp");
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


    public void printMutation(Mutation mutation) {
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

    public boolean sendToOthers(Mutation mutation) {
        if (isDataMutation(mutation)) {
            System.out.println("Send to others");
            return true;
        } else {
            System.out.println("Don't send to others");
            return false;
        }
    }

    public boolean isDataMutation(IMutation mutation) {
        //check if the Mutation (our time) only needs to perform locally by checking if it has the sendFrom column
        Row mutationRow = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        for (Cell c : mutationRow.cells())
        {
            String colName = c.column().name.toString();
            if (colName.equals("sendfrom"))
            {
                System.out.println("Contains sendfrom");
                return true;
            }
        }
        return false;
    }

    public boolean isVectorInitiate() {
        System.out.println("Inside is VectorInitiate");
        TableMetadata timeVectorMeta = SchemaKeyspace.metadata().getTableOrViewNullable("server");
        SinglePartitionReadCommand localRead = SinglePartitionReadCommand.fullPartitionRead(
        timeVectorMeta,
        FBUtilities.nowInSeconds(),
        ByteBuffer.wrap(Integer.toString(CausalUtility.getWriterID()).getBytes())
        );

        try (ReadExecutionController executionController = localRead.executionController();
             UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController))
        {

            // first we have to transform it into a PartitionIterator
            PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());

            // if the db does not have the data, it will not go through this while loop
            while(pi.hasNext())
            {
                RowIterator ri = pi.next();
                while(ri.hasNext())
                {
                    return true;
                }
            }
        }

        return false;
    }

    public List<Integer> fetchMyTimeStamp() {
        //fetch my timeStamp
        // for each muation we first read the coresponding timestamp
        //Fetch the metadata of our Timutation
        TableMetadata timeVectorMeta = SchemaKeyspace.metadata().getTableOrViewNullable("server");
        List<Integer> myTimeStamp = new ArrayList<>();
        SinglePartitionReadCommand localRead = SinglePartitionReadCommand.fullPartitionRead(
        timeVectorMeta,
        FBUtilities.nowInSeconds(),
        ByteBuffer.wrap(Integer.toString(CausalUtility.getWriterID()).getBytes())
        );

        int local_vector_entry_time;

        try (ReadExecutionController executionController = localRead.executionController();
             UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController))
        {

            // first we have to transform it into a PartitionIterator
            PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());

            // if the db does not have the data, it will not go through this while loop
            while(pi.hasNext())
            {
                // zValueReadResult.next() returns a RowIterator
                RowIterator ri = pi.next();
                while(ri.hasNext())
                {
                    // fetch the current row;
                    Row r = ri.next();
                    // Fetch the current_local_timestamp from individual columns
                    // individual columns represents individual server's writing timeStamp;
                    for (int id = 0; id < CausalUtility.getNumNodes(); id++)
                    {
                        String colName = CausalUtility.getColPrefix() + id;
                        ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(colName));
                        Cell c = r.getCell(colMeta);
                        local_vector_entry_time = ByteBufferUtil.toInt(c.value());
                        System.out.println("Mytime entry: " + local_vector_entry_time );
                        myTimeStamp.add(local_vector_entry_time);
                    }

                }
            }
        }
        return myTimeStamp;
    }

    public Mutation updateLocalTimeStamp(List<Integer> myTimeStamp, Mutation mutation) {
        TableMetadata timeVectorMeta = SchemaKeyspace.metadata().getTableOrViewNullable("server");
        DecoratedKey myKey = timeVectorMeta.partitioner.decorateKey(ByteBuffer.wrap(Integer.toString(CausalUtility.getWriterID()).getBytes()));
        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(mutation.getKeyspaceName(), myKey);
        for (int id = 0; id < CausalUtility.getNumNodes(); id++)
        {
            String colName = CausalUtility.getColPrefix() + id;
            int time = myTimeStamp.get(id);
            if (id == CausalUtility.getWriterID())
            {
                myTimeStamp.set(id, myTimeStamp.get(id) + 1);
                time = myTimeStamp.get(id);
            }
            mutationBuilder.update(timeVectorMeta)
                           .timestamp(FBUtilities.timestampMicros())
                           .row()
                           .add(colName, time);
        }
        return mutationBuilder.build();
    }

}
