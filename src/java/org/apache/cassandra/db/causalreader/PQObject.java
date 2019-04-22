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

import java.util.List;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;

public class PQObject
{
    private List<Integer> mutationTimeStamp;
    private long physicalTimeStamp;
    private Mutation mutation;
    private  int senderID;
    private int id ;
    private InetAddressAndPort replyto;

    public PQObject(List<Integer>mutationTimeStamp, long physicalTimeStamp, Mutation mutation, int senderID, int id, InetAddressAndPort replyto) {
        this.mutationTimeStamp = mutationTimeStamp;
        this.physicalTimeStamp = physicalTimeStamp;
        this.mutation = mutation;
        this.senderID = senderID;
        this.replyto = replyto;
        this.id = id;
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public long getPhysicalTimeStamp()
    {
        return physicalTimeStamp;
    }

    public List<Integer> getMutationTimeStamp()
    {
        return mutationTimeStamp;
    }

    public int getSenderID() {
        return this.senderID;
    }

    public int getId() {
        return this.id;
    }

    public InetAddressAndPort getReplyto() {
        return this.replyto;
    }
}

