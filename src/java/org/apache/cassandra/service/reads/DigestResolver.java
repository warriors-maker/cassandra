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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.Treas.DoubleTreasTag;
import org.apache.cassandra.Treas.ErasureCode;
import org.apache.cassandra.Treas.FetchTagObject;
import org.apache.cassandra.Treas.TreasConfig;
import org.apache.cassandra.Treas.TreasTag;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ABDColomns;
import org.apache.cassandra.service.ABDTag;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

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

    // this is the original method, NoopReadRepair has a call to this method
    // simply change the method signature to ReadResponse getData() will raise an compiler error
    public PartitionIterator getData()
    {
        assert isDataPresent();
        return UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
    }

    // this is a new method for AbstractReadExecutor, which may want to use ReadResponse more than once
    public ReadResponse getReadResponse()
    {
        assert isDataPresent();
        return dataResponse;
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
        ABDTag maxTag = new ABDTag();
        ReadResponse maxZResponse = null;

        ColumnIdentifier zIdentifier = new ColumnIdentifier(ABDColomns.TAG, true);
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

                    ABDTag curTag = new ABDTag();
                    for(Cell c : ri.next().cells())
                    {
                        if(c.column().name.equals(zIdentifier)) {
                            curTag = ABDTag.deserialize(c.value());
                        }
                    }

                    if(curTag.isLarger(maxTag))
                    {
                        maxTag = curTag;
                        maxZResponse = response;
                    }
                }
            }
        }
        return maxZResponse;
    }

    public void fetchTag(FetchTagObject coordinatorInfo) {
        TreasTag localMaxTreasTag = new TreasTag();

        // Each readResponse represents a response from a Replica
        for (MessageIn<ReadResponse> message : this.getMessages())
        {

            ReadResponse response = message.payload;

            // check if the response is indeed a data response
            // we shouldn't get a digest response here
            assert response.isDigestResponse() == false;

            // get the partition iterator corresponding to the
            // current data response
            PartitionIterator pi = UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());

            String minTagColName = null;
            String maxFieldColName = null;
            TreasTag maxCoordinatorTag = null;
            TreasTag minCoodinatorTag = null;
            String minFieldColName = null;

            boolean myMessage = false;
            int hit = 1;

            //minTagColName == null => first time see this data

            if (message.from.equals(FBUtilities.getLocalAddressAndPort())) {
//                logger.debug("My message");
                myMessage = true;
            }

            while (pi.hasNext())
            {
                // pi.next() returns a RowIterator
                RowIterator ri = pi.next();
                while (ri.hasNext())
                {
                    TreasTag curTag = new TreasTag();
                    for (Cell c : ri.next().cells())
                    {
                        //TODO: Inside here we can fetch the corresponding tag the coordinator should insert
                        // if it is a timeStamp field, we need to check it
                        String colName = c.column().name.toString();
                        if (colName.startsWith("tag"))
                        {
                            curTag = TreasTag.deserialize(c.value());
                            if (myMessage) {
//                                logger.debug(curTag.toString());
                                hit++;
                                if (minCoodinatorTag == null) {
                                    minCoodinatorTag = curTag;
                                    minTagColName = colName;
                                    maxCoordinatorTag = curTag;
                                    maxFieldColName = "field" + colName.substring(3);
                                    minFieldColName = "field" + colName.substring(3);
                                }
                                else if (minCoodinatorTag.isLarger(curTag)) {
                                    minCoodinatorTag = curTag;
                                    minTagColName = colName;
                                    minFieldColName = "field" + colName.substring(3);
                                }
                                else if (curTag.isLarger(maxCoordinatorTag)) {
                                    maxCoordinatorTag = curTag;
                                    maxFieldColName = "field" + colName.substring(3);
                                }
                            }
                            if (curTag.isLarger(localMaxTreasTag))
                            {
                                localMaxTreasTag = curTag;
                            }
                        }
                    }
                }
            }

            if (myMessage) {
                if (hit <= TreasConfig.num_concurrecy) {
                    coordinatorInfo.minTagColName = "tag" + hit;
                    coordinatorInfo.minFieldColName = "field" + hit;
                } else {
                    coordinatorInfo.minCoodinatorTag = minCoodinatorTag;
                    coordinatorInfo.minTagColName = minTagColName;
                    coordinatorInfo.minFieldColName = minFieldColName;
                }
                coordinatorInfo.maxCoordinatorTag = maxCoordinatorTag;
                coordinatorInfo.maxFieldColName = maxFieldColName;
                coordinatorInfo.hit = hit;
            }

            myMessage = false;
        }

        coordinatorInfo.maxTagAll = new TreasTag(localMaxTreasTag);
    }

    public void fetchTargetTags(DoubleTreasTag doubleTreasTag) {
        //logger.debug("Inside awaitResponsesTreasTagValue");
        //System.out.println("Inside awaitResponsesTreasTagValue");
//        long startTime = System.nanoTime();
        HashMap<TreasTag, Integer> quorumMap = new HashMap<>();

        HashMap<TreasTag, List<String>> decodeMap = new HashMap<>();
        HashMap<TreasTag, Integer> decodeCountMap = new HashMap<>();

        TreasTag quorumTagMax = new TreasTag();
        TreasTag decodeTagMax = new TreasTag();

        // This needs to be a Map<Integer, String>:
        // Integer represents the ID of server
        // String is just the value
        List<String> decodeValMax = null;


        String keySpaceName = "";
        DecoratedKey key = null;
        TableMetadata tableMetadata = null;

        //logger.debug("Before Digest Match");
        //logger.debug("Message size is" + this.getMessages().size());
        // Each readResponse represents a response from a Replica

        HashMap<String, Integer> addressMap = TreasConfig.getAddressMap();

        for (MessageIn<ReadResponse> message : this.getMessages())
        {
            String address = message.from.address.toString().substring(1);
            //logger.debug("Address are" + address);
            int id = addressMap.get(address);
            //logger.debug("The message is from" + address + "ID is: " + id);

            ReadResponse response = message.payload;

            assert response.isDigestResponse() == false;

            // get the partition iterator corresponding to the
            // current data response
            PartitionIterator pi = UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());

            while (pi.hasNext())
            {
                // pi.next() returns a RowIterator
                RowIterator ri = pi.next();

                if (keySpaceName.equals(""))
                {
                    key = ri.partitionKey();
                    tableMetadata = ri.metadata();
                    keySpaceName = tableMetadata.keyspace;
                    doubleTreasTag.setKey(key);
                    doubleTreasTag.setTableMetadata(tableMetadata);
                    doubleTreasTag.setKeySpace(keySpaceName);
                }

                while (ri.hasNext())
                {
                    Row row = ri.next();
                    for (Cell c : row.cells())
                    {
                        String colName = c.column().name.toString();

                        // if it is a timeStamp field, we need to check it
                        if (colName.startsWith("tag"))
                        {

                            TreasTag curTag = TreasTag.deserialize(c.value());

                            if (quorumMap.containsKey(curTag))
                            {
                                int currentCount = quorumMap.get(curTag) + 1;
                                quorumMap.put(curTag, currentCount);
                                // if has enough k values
                                if (currentCount == TreasConfig.num_intersect)
                                {
                                    if (curTag.isLarger(quorumTagMax))
                                    {
                                        quorumTagMax = curTag;
                                    }
                                }
                            }
                            else
                            {
                                quorumMap.put(curTag, 1);
                                if (TreasConfig.num_intersect == 1)
                                {
                                    if (curTag.isLarger(quorumTagMax))
                                    {
                                        quorumTagMax = curTag;
                                    }
                                }
                            }
                        }
                        // Notice that only one column has the data
                        else if (colName.startsWith("field") && !colName.equals("field0"))
                        {
                            // Find the corresponding index to fetch the tag value
                            int index = Integer.parseInt(colName.substring(TreasConfig.VAL_PREFIX.length()));
                            String treasTagColumn = "tag" + index;
                            ColumnIdentifier tagOneIdentifier = new ColumnIdentifier(treasTagColumn, true);
                            ColumnMetadata columnMetadata = ri.metadata().getColumn(tagOneIdentifier);
                            Cell tagCell = row.getCell(columnMetadata);
                            TreasTag treasTag = TreasTag.deserialize(tagCell.value());

                            if (decodeCountMap.get(decodeTagMax) != null && treasTag.equals(decodeTagMax) && decodeCountMap.get(decodeTagMax) >= TreasConfig.num_recover) {
                                int count = decodeCountMap.get(decodeTagMax) + 1;
                                decodeCountMap.put(decodeTagMax, count);
                                if (count == TreasConfig.QUORUM) {
                                    doubleTreasTag.setNeedWriteBack(false);
                                }
                                continue;
                            }
                            // Fetch the code out
                            String value = "";
                            try
                            {
                                value = ByteBufferUtil.string(c.value());
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();
                            }


                            if (decodeMap.containsKey(treasTag))
                            {
                                List<String> codeList = decodeMap.get(treasTag);
                                //logger.debug("CodeList size is" + codeList.size());
                                codeList.set(id, value);

                                int count = decodeCountMap.get(treasTag) + 1;
                                decodeCountMap.put(treasTag,count);

//                                decodeMap.get(treasTag).set(id, value);

                                if (count == TreasConfig.num_recover)
                                {
                                    if (treasTag.isLarger(decodeTagMax))
                                    {
                                        decodeTagMax = treasTag;
                                        decodeValMax = codeList;
                                    }
                                }

                                if (count == TreasConfig.QUORUM) {
                                    doubleTreasTag.setNeedWriteBack(false);
                                }
                            }
                            else
                            {
                                List<String> codelist = Arrays.asList(new String[TreasConfig.num_server]);
                                //logger.debug("Initialize the codelist and size is " + codelist.size());
                                codelist.set(id, value);
                                decodeMap.put(treasTag, codelist);
                                decodeCountMap.put(treasTag,1);
                                if (TreasConfig.num_recover == 1)
                                {
                                    if (treasTag.isLarger(decodeTagMax))
                                    {
                                        decodeTagMax = treasTag;
                                        decodeValMax = codelist;
                                    }
                                }
                                if (TreasConfig.QUORUM == 1) {
                                    doubleTreasTag.setNeedWriteBack(false);
                                }
                            }
                        }
                    }
                }
            }
        }

        //logger.debug("Finish reading Quorum and Decodable");
        //System.out.println(quorumTagMax.getTime() + "," + decodeTagMax.getTime());
        //logger.debug(quorumTagMax.getTime() + "," + decodeTagMax.getTime());

        // Either one of them is not satisfied stop the procedure;
        if (quorumTagMax.getTime() == -1 || decodeTagMax.getTime() == -1)
        {
            doubleTreasTag.setReadResult(null);
        }
        else
        {
            //logger.debug("Successfully get the result");
            doubleTreasTag.getQuorumMaxTreasTag().setWriterId(quorumTagMax.getWriterId());
            doubleTreasTag.getQuorumMaxTreasTag().setLogicalTIme(quorumTagMax.getTime());
            doubleTreasTag.getRecoverMaxTreasTag().setWriterId(decodeTagMax.getWriterId());
            doubleTreasTag.getRecoverMaxTreasTag().setLogicalTIme(decodeTagMax.getTime());

            int length = 0;
            for (int i = 0; i < decodeValMax.size(); i++) {
                String value = decodeValMax.get(i);
                if (value != null && ! value.isEmpty()) {
                    length = TreasConfig.stringToByte(value).length;
                    break;
                }
            }


            boolean []shardPresent = new boolean[TreasConfig.num_server];
            byte[][] decodeMatrix = new byte[TreasConfig.num_server][length];

            int count = 0;

            for (int i = 0; i < decodeValMax.size(); i++) {

                String value = decodeValMax.get(i);

                if (value == null || value.isEmpty() || count == TreasConfig.num_recover) {
                    decodeMatrix[i] = new byte[length];
                }

                else {
                    count++;
                    byte[] replica_array = TreasConfig.stringToByte(value);
                    decodeMatrix[i] = replica_array;
                    shardPresent[i] = true;
                }
            }

            String value = ErasureCode.decodeeData(decodeMatrix, shardPresent, length);
            //logger.debug("Convert the data to value" + value);
            doubleTreasTag.setReadResult(value);
        }

    }



    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
