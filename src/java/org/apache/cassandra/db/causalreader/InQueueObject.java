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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.ByteBufferUtil;


public class InQueueObject
{
    // need to change this to Mutation type not message Type
    private int id;
    private InetAddressAndPort replyTo;
    private MessageIn<Mutation> message;
    private static final Logger logger = LoggerFactory.getLogger(InQueueObject.class);
    private Mutation mutation;
    private List<Integer> mutationTimeStamp;
    private int senderID;
    private long physicalTimeStamp;


    public InQueueObject (MessageIn<Mutation> message, int id, InetAddressAndPort replyTo, long physicalTimeStamp) {
        this.message = message;
        this.id = id;
        this.replyTo = replyTo;
        this.mutation = message.payload;
        this.mutationTimeStamp = new ArrayList<>();
        this.physicalTimeStamp = physicalTimeStamp;
    }

    public Mutation getMutation() {
        return this.mutation;
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


    public MessageIn<Mutation> getMessage()
    {
        return message;
    }

    public long getPhysicalTimeStamp()
    {
        return physicalTimeStamp;
    }


}