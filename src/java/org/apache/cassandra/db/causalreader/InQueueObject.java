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


import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteResponse;
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
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class InQueueObject
{
    // need to change this to Mutation type not message Type
    private Mutation mutation;
    private List<Integer> localTimeStamp;
    private List<Integer> mutationTimeStamp;
    private int id;
    private InetAddressAndPort replyTo;
    private MessageIn<Mutation> message;
    private int senderID;

    public InQueueObject (MessageIn<Mutation> message, int id, InetAddressAndPort replyTo) {
        this.message = message;
        this.mutation = message.payload;
        this.id = id;
        this.replyTo = replyTo;

    }

    public Mutation getMutation() {
        return this.mutation;
    }

    public List<Integer> getLocalTimeStamp() {
        return this.localTimeStamp;
    }

    public List<Integer> getMutationTimeStamp() {
        return this.mutationTimeStamp;
    }

    public int getId()
    {
        return id;
    }

    public int getSenderID() {
        return this.senderID;
    }

    public InetAddressAndPort getReplyTo() {
        return this.replyTo;
    }

    private void setLocalTimeStamp(List<Integer> localTimeStamp) {
        this.localTimeStamp = localTimeStamp;
    }

    private void setMutationTimeStamp(List<Integer> mutationTimeStamp) {
        this.mutationTimeStamp = mutationTimeStamp;
    }

    public MessageIn<Mutation> getMessage()
    {
        return message;
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    // fetch mutation timestamp and local timestamp
    public void fetchTimeStamp () {
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

            //Extract tag information from the local read into a Vector (List) timeStamp
            List<Integer> local_timeStamp = new ArrayList<>();
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
                        Row localRow = ri.next();

                        //fetch the senderID for this mutation
                        String colName = CausalUtility.getSenderColName();
                        ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(colName));
                        Cell c = localRow.getCell(colMeta);
                        int senderID = ByteBufferUtil.toInt(c.value());
                        this.senderID = senderID;

                        //Fetch the server timeStamp
                        //Fetch the mutation TimeStamp
                        for (int server_id = 0; server_id < CausalUtility.getNumNodes(); server_id++)
                        {
                            // Read through individual column, which are the time_Vector_Entry
                            colName = CausalUtility.getColPrefix() + server_id;

                            // reading the current Server TimeStamp;
                            colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(colName));
                            c = localRow.getCell(colMeta);
                            int local_vector_col_time = ByteBufferUtil.toInt(c.value());
                            // Whenever there is a mutation on current server, update its corresponding timeStamp
                            local_timeStamp.add(local_vector_col_time);

                        }
                    }
                }
            }

            //fetch the rowMutation information
            List<Integer> mutation_timeStamp = new ArrayList<>();
            Row mutationRow = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);

            int id = 0;
            // TODO: Need to check whether it is in the order we define the schema
            for (Cell c : mutationRow.cells()) {
                System.out.println(c.column().name.toString());
                if(c.column().name.equals(new ColumnIdentifier(CausalUtility.getColPrefix() + id, true))) {
                    mutation_timeStamp.add(ByteBufferUtil.toInt(c.value()));
                }
            }
            this.setLocalTimeStamp(local_timeStamp);
            this.setMutationTimeStamp(mutation_timeStamp);
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }
}
