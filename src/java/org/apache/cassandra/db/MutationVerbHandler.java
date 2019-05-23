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
import java.util.Iterator;

import org.apache.cassandra.Treas.TreasConfig;
import org.apache.cassandra.Treas.TreasTag;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    private void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException {
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

        int hit = 1;
        boolean exist = false;
        String minTagColumn = "";
        String maxTagColumn ="";
        TreasTag localMinTag = null;
        TreasTag localMaxTag = null;

        TreasTag mutationTag = null;
        String mutationValue = "";

        // Read from the Mutation
        Row data = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        for (Cell c : data.cells())
        {
            if (c.column().name.toString().equals("tag1")) {
                mutationTag = TreasTag.deserialize(c.value());
            } else if (c.column().name.toString().equals("field1")) {
                mutationValue = ByteBufferUtil.string(c.value());
            }
        }

        // Read the value from the mutation
        // The tag sent from coordinator is always in tag1 and value is always in field 1


        // Read thew smallest Tag from the column and get where the value column is
        SinglePartitionReadCommand localRead =
        SinglePartitionReadCommand.fullPartitionRead(
        message.payload.getPartitionUpdates().iterator().next().metadata(),
        FBUtilities.nowInSeconds(),
        message.payload.key()
        );


        // Read from local
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

                    for (Cell cell : r.cells()) {
                        String colName = cell.column().name.toString();

                        if (colName.startsWith("tag")) {
                            TreasTag currentTag = TreasTag.deserialize(cell.value());
                            hit++;
                            if (currentTag.equals(mutationTag)) {
                                exist = true;
                                break;
                            }
                            if (localMaxTag == null && localMinTag == null) {
                                localMaxTag = currentTag;
                                maxTagColumn = colName;
                                localMinTag = currentTag;
                                minTagColumn = colName;
                            }
                            else {
                                if (currentTag.isLarger(localMaxTag)) {
                                    localMaxTag = currentTag;
                                    maxTagColumn = colName;
                                }
                                else if (localMinTag.isLarger(currentTag)) {
                                    localMinTag = currentTag;
                                    minTagColumn = colName;
                                }

                            }
                        }
                    }
                }
            }
        }

        // The Tag Already exists, no need to write into the disk;
        if (exist) {
            reply(id, replyTo);
            return;
        }
        Mutation mutation = message.payload;
        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(mutation.getKeyspaceName(), mutation.key());
        TableMetadata tableMetadata = mutation.getPartitionUpdates().iterator().next().metadata();
        long timeStamp = FBUtilities.timestampMicros();

        // Meaning one of the column is missing
        if (hit <= TreasConfig.num_concurrecy) {
            // if hit == 1:
            // No data yet, directly commit the
            if (hit == 1) {
                message.payload.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
                    failed();
                    return null;
                });
                return;
            } else {
                // If larger, we need to
                // First add to the hit spot (Both Tag and Value)
                // Remove value
                if (mutationTag.isLarger(localMaxTag)) {
                    mutationBuilder.update(tableMetadata)
                                   .timestamp(timeStamp)
                                   .row()
                                   .add("field0","")
                                   .add("tag" + hit, TreasTag.serialize(mutationTag))
                                   .add("field" + hit, mutationValue)
                                   .add("field" + maxTagColumn.substring(3),null);
                }
                // If not larger
                // Just need to add to the hit spot (only Tag)
                else {
                    mutationBuilder.update(tableMetadata)
                                   .timestamp(timeStamp)
                                   .row()
                                   .add("field0","")
                                   .add("tag" + hit, TreasTag.serialize(mutationTag));
                }
            }
        }

        else {
            if (mutationTag.isLarger(localMaxTag)) {
                mutationBuilder.update(tableMetadata)
                               .timestamp(timeStamp)
                               .row()
                               .add("field" + maxTagColumn.substring(3),null)
                               .add("field" + minTagColumn.substring(3), mutationValue)
                               .add(minTagColumn,TreasTag.serialize(mutationTag));
            }

            else if (mutationTag.isLarger(localMinTag)){
                mutationBuilder.update(tableMetadata)
                               .timestamp(timeStamp)
                               .row()
                               .add(minTagColumn, TreasTag.serialize(mutationTag));
            } else {
                reply(id, replyTo);
                return;
            }
        }

        Mutation commitMutation = mutationBuilder.build();
        // Commit this mutation
        commitMutation.applyFuture().thenAccept(o -> reply(id, replyTo)).exceptionally(wto -> {
            failed();
            return null;
        });
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
