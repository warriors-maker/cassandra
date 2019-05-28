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

package org.apache.cassandra.Treas;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class TreasTagMap
{
    private HashMap<TreasTag, String> localData = null;
    // Only if we have not seen this data will this mutation be return;


    public synchronized TreasValueID readTag() {
        TreasTag[] tagList = localData.keySet().toArray(new TreasTag[localData.keySet().size()]);

        String value = "";
        int id = 0;

        TreasTag maxTag = null;

        for (Map.Entry entry : localData.entrySet()) {
            value = (String) entry.getValue();
            if (!value.isEmpty()) {
                maxTag = (TreasTag) entry.getKey();
                break;
            }
            id ++;
        }

        return new TreasValueID(id, value, tagList, maxTag);
    }

    public synchronized Mutation putTreasTag(TreasTag mutationTag, String value, Mutation mutation) {
        // Haven't seen this data
        if (localData == null) {
            localData = new HashMap<>();
            localData.put(mutationTag, value);

            Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(mutation.getKeyspaceName(), mutation.key());
            TableMetadata tableMetadata = mutation.getPartitionUpdates().iterator().next().metadata();
            long timeStamp = FBUtilities.timestampMicros();
            for (int i = 0 ; i <= TreasConfig.num_concurrecy; i++)
            {
                if (i == 0)
                {
                    mutationBuilder
                    .update(tableMetadata)
                    .timestamp(timeStamp)
                    .row()
                    .add(TreasConfig.VAL_PREFIX + i, "");
                }
                else
                {
                    mutationBuilder
                    .update(tableMetadata)
                    .timestamp(timeStamp)
                    .row()
                    .add(TreasConfig.TAG_PREFIX + i, "")
                    .add(TreasConfig.VAL_PREFIX + i, "");
                }
            }
            Mutation commitMutation = mutationBuilder.build();
            return commitMutation;
        }
        // We have seen this data before
        else {
            // Check if the tag exists or not
            if (localData.containsKey(mutationTag)) {
                return null;
            }

            TreasTag minTag = null;
            TreasTag maxTag = null;

            for (Map.Entry entry : localData.entrySet()) {
                TreasTag curTag = (TreasTag) entry.getKey();

                if (minTag == null) {
                    minTag = curTag;
                    maxTag = curTag;
                }
                else if (curTag.isLarger(maxTag)) {
                    maxTag = curTag;
                }
                else if (minTag.isLarger(curTag)) {
                    minTag = curTag;
                }
            }

            // Directly add into the map
            if (localData.size() < TreasConfig.num_concurrecy) {
                // If it is larger than any tag we have seen
                if (mutationTag.isLarger(maxTag)) {
                    localData.put(maxTag,"");
                    localData.put(mutationTag, value);
                }
                // if it is smaller than the maxTag we have seen
                else {
                    localData.put(mutationTag,"");
                }
            }

            // If Exceed the number of concurrecy
            else {
                // if smaller than anyone of the tag we have seen
                if (minTag.isLarger(mutationTag)) {
                    return null;
                }
                // if incoming MutationTag is the largest
                else if (mutationTag.isLarger(maxTag)) {
                    localData.put(maxTag, "");
                    localData.put(mutationTag, value);
                    localData.remove(minTag);
                }
                // If incoming MutationTag is not the largest
                else {
                    localData.put(mutationTag,"");
                    localData.remove(minTag);
                }
            }
        }
        return null;
    }

    public synchronized void printTagMap() {
        for (Map.Entry entry : localData.entrySet()) {
            System.out.print("TreasInfo: ");
            System.out.print(entry.getKey() + " : " + entry.getValue() + " || ");
        }
        System.out.println();
    }
}
