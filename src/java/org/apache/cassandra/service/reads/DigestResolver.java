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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DigestResolver extends ResponseResolver
{
    private volatile ReadResponse dataResponse;

    public DigestResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReadRepair readRepair, int maxResponseCount)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        if (dataResponse == null && !message.payload.isDigestResponse())
            dataResponse = message.payload;
    }

    public PartitionIterator getData()
    {
        assert isDataPresent();
        return UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest = response.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return true;
    }

    public ReadResponse extractMaxZResponse()
    {
        // check all data responses,
        // extract the one with max z value
        int maxZ = -1;
        String maxWriterId = ""; // when we compare writer id, we use lexicographical order
        ReadResponse maxZResponse = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;

            // check if the response is indeed a data response
            // we shouldn't get a digest response here
            assert response.isDigestResponse() == false;

            // get the partition iterator corresponding to the
            // current data response
            PartitionIterator pi = UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());

            // get the z value column
            while(pi.hasNext())
            {
                // zValueReadResult.next() returns a RowIterator
                RowIterator ri = pi.next();
                while(ri.hasNext())
                {
                    // todo: the entire row is read for the sake of development
                    // future improvement could be made

                    int currentZ = -1;
                    String writerId = "";
                    for(Cell c : ri.next().cells())
                    {
                        if(c.column().name.equals(new ColumnIdentifier("z_value", true)))
                        {
                            currentZ = ByteBufferUtil.toInt(c.value());
                        }
                        else if(c.column().name.equals(new ColumnIdentifier("writer_id", true)))
                        {
                            try
                            {
                                writerId = ByteBufferUtil.string(c.value());
                            }
                            catch(Exception e)
                            {}
                        }
                    }

                    /*
                    ColumnMetadata colMeta = command.metadata().getColumn(ByteBufferUtil.bytes("z_value"));
                    Cell c = ri.next().getCell(colMeta);
                    int currentZ = ByteBufferUtil.toInt(c.value());
                    */

                    if(currentZ > maxZ)
                    {
                        maxZ = currentZ;
                        maxWriterId = writerId;
                        maxZResponse = response;
                    }
                    else if(currentZ == maxZ)
                    {
                        // compare writer id if z value is the same
                        if(writerId.compareTo(maxWriterId) > 0)
                        {
                            maxWriterId = writerId;
                            maxZResponse = response;
                        }
                    }
                }
            }
        }

        return maxZResponse;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
