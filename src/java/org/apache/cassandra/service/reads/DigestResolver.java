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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.Treas.DoubleTreasTag;
import org.apache.cassandra.Treas.ErasureCode;
import org.apache.cassandra.Treas.TreasConfig;
import org.apache.cassandra.Treas.TreasMap;
import org.apache.cassandra.Treas.TreasTag;
import org.apache.cassandra.Treas.TreasTagMap;
import org.apache.cassandra.Treas.TreasValueID;
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
            while (pi.hasNext())
            {
                // zValueReadResult.next() returns a RowIterator
                RowIterator ri = pi.next();
                while (ri.hasNext())
                {
                    // todo: the entire row is read for the sake of development
                    // future improvement could be made

                    ABDTag curTag = new ABDTag();
                    for (Cell c : ri.next().cells())
                    {
                        if (c.column().name.equals(zIdentifier))
                        {
                            curTag = ABDTag.deserialize(c.value());
                        }
                    }

                    if (curTag.isLarger(maxTag))
                    {
                        maxTag = curTag;
                        maxZResponse = response;
                    }
                }
            }
        }
        return maxZResponse;
    }

    public void fetchTargetTagValue(DoubleTreasTag doubleTreasTag)
    {
        //logger.debug("Inside awaitResponsesTreasTagValue");

        //System.out.println("Inside awaitResponsesTreasTagValue");

        //logger.debug("Inside awaitResponsesTreasTagValue");
        //System.out.println("Inside awaitResponsesTreasTagValue");
        HashMap<TreasTag, Integer> quorumMap = new HashMap<>();

        HashMap<TreasTag, List<String>> decodeMap = new HashMap<>();
        HashMap<TreasTag, Integer> decodeCountMap = new HashMap<>();

        TreasTag quorumTagMax = new TreasTag();
        TreasTag decodeTagMax = new TreasTag();

        // This needs to be a Map<Integer, String>:
        // Integer represents the ID of server
        // String is just the value
        List<String> decodeValMax = null;

        HashMap<String, Integer> addressMap = TreasConfig.getAddressMap();


        TreasTagMap localTagMap = TreasMap.getInternalMap().putIfAbsent(doubleTreasTag.getKey().toString(), new TreasTagMap());
        if (localTagMap == null) {

        } else
        {
            TreasValueID obj = localTagMap.readTag();
            TreasTag localTag = obj.maxTag;
            String value = obj.value;


            List<String> codeList = Arrays.asList(new String[TreasConfig.num_server]);

            int myIndex = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));

            codeList.set(myIndex, value);
            decodeMap.put(localTag, codeList);
            decodeCountMap.put(localTag, 1);
            quorumMap.put(localTag, 1);

            if (TreasConfig.num_intersect == 1)
            {
                quorumTagMax = localTag;
            }

            if (TreasConfig.num_recover == 1)
            {
                decodeTagMax = localTag;
                decodeValMax = codeList;
                doubleTreasTag.setNeedWriteBack(false);
            }
        }



        for (MessageIn<ReadResponse> message : this.getMessages())
        {
            String address = message.from.address.toString().substring(1);

            int addressId = addressMap.get(address);
            //logger.debug("The message is from" + address + "ID is: " + id);

            ReadResponse response = message.payload;

            TreasTag[] taglist = response.tagList;
            // The value corresponding to the tagList index
            int valueIndex = response.index;
            // the actuall value;
            String val = response.val;

            logger.debug("Get from ReadResponse" + valueIndex + " " + val);

            if (taglist == null) {
                continue;
            }

            for (int i = 0; i < taglist.length; i++)
            {
                logger.debug(taglist[i].toString());
            }

            // Fetch the Quorum to satisfy the condition
            for (TreasTag tag : taglist)
            {
                if (quorumMap.containsKey(tag))
                {
                    quorumMap.put(tag, quorumMap.get(tag) + 1);
                }
                else
                {
                    quorumMap.put(tag, 1);
                }
                if (quorumMap.get(tag) == TreasConfig.num_intersect)
                {
                    if (tag.isLarger(quorumTagMax))
                    {
                        quorumTagMax = tag;
                    }
                }
            }

            // Recover
            List<String> codeList;
            TreasTag valueTag = taglist[valueIndex];
            // Set the value According to its relative address Order
            if (decodeMap.containsKey(valueTag))
            {
                codeList = decodeMap.get(valueTag);
                codeList.set(addressId, val);
            }
            else
            {
                codeList = Arrays.asList(new String[TreasConfig.num_server]);
            }
            codeList.set(addressId, val);

            int count;
            if (decodeCountMap.containsKey(valueTag))
            {
                int localCount = decodeCountMap.get(valueTag) + 1;
                decodeCountMap.put(valueTag, localCount);
                count = localCount;
            }
            else
            {
                decodeCountMap.put(valueTag, 1);
                count = 1;
            }

            if (count == TreasConfig.num_recover)
            {
                if (valueTag.isLarger(decodeTagMax))
                {
                    decodeTagMax = valueTag;
                    decodeValMax = decodeMap.get(decodeTagMax);
                }
            }

            // If Majority get all the same Tag, no need to write back;
            if (count == TreasConfig.QUORUM)
            {
                doubleTreasTag.setNeedWriteBack(false);
            }
        }

        if (decodeValMax == null)
        {
            return;
        }

        int length = 0;
        for (int i = 0; i < decodeValMax.size(); i++)
        {
            String value = decodeValMax.get(i);
            if (value != null && !value.isEmpty())
            {
                length = TreasConfig.stringToByte(value).length;
                break;
            }
        }

        boolean[] shardPresent = new boolean[TreasConfig.num_server];
        byte[][] decodeMatrix = new byte[TreasConfig.num_server][length];

        int count = 0;

        for (int i = 0; i < decodeValMax.size(); i++)
        {

            String value = decodeValMax.get(i);

            if (value == null || value.isEmpty() || count == TreasConfig.num_recover)
            {
                decodeMatrix[i] = new byte[length];
            }
            else
            {
                count++;
                byte[] replica_array = TreasConfig.stringToByte(value);
                decodeMatrix[i] = replica_array;
                shardPresent[i] = true;
            }
        }

        // Decode here
        String value = ErasureCode.decodeeData(decodeMatrix, shardPresent, length);
        logger.debug("Convert the data to value" + value);
        doubleTreasTag.setReadValue(value);
    }


//            if (!initialized) {
//                TreasTagMap localTagMap = TreasMap.getInternalMap().putIfAbsent(key, new TreasTagMap());
//
//                if (localTagMap == null) {
//
//                } else {
//                    TreasValueID obj = localTagMap.readTag();
//                    TreasTag localTag = obj.maxTag;
//                    String value = obj.value;
//
//
//                    List<String> codeList = Arrays.asList(new String[TreasConfig.num_server]);
//
//                    int myIndex = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//
//                    codeList.set(myIndex, value);
//                    decodeMap.put(localTag,codeList);
//                    decodeCountMap.put(localTag,1);
//                    quorumMap.put(localTag,1);
//
//                    //logger.debug(localTag.toString());
//
//                    if (TreasConfig.num_intersect == 1) {
//                        quorumTagMax = localTag;
//                    }
//
//                    if (TreasConfig.num_recover == 1) {
//                        decodeTagMax = localTag;
//                        decodeValMax = codeList;
//                    }
//                    initialized = true;
//                }
//            }


    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}

