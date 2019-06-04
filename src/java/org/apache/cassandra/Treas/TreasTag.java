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


import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreasTag implements Serializable{
    private int logicalTIme;
    private String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(TreasTag.class);

    public TreasTag(){
        this.logicalTIme = -1;
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
//        logger.info(this.toString());
    }

    private TreasTag(String tagString){
        String[] tagArray = tagString.split(";");
        this.logicalTIme = Integer.parseInt(tagArray[0]);
        this.writerId = tagArray[1];
//        logger.info(this.toString());
    }

    public void setLogicalTIme(int logicalTIme) {
        this.logicalTIme = logicalTIme;
    }

    public void setWriterId(String writerId) {
        this.writerId = writerId;
    }

    public int getTime(){
        return logicalTIme;
    }

    public String getWriterId() {
        return writerId;
    }

    public TreasTag nextTag(){
        this.logicalTIme++;
//        logger.info(this.toString());
        return this;
    }

    public static String serialize(TreasTag tag) {
        return tag.toString();
    }

    public static TreasTag deserialize(ByteBuffer buf) {
        String tagString = "";
        try {
            tagString = ByteBufferUtil.string(buf);
        } catch (CharacterCodingException e){
            logger.warn("err casting tag {}",e);
            return new TreasTag();
        }

        return new TreasTag(tagString);
    }

    public boolean isLarger(TreasTag other){
        if(this.logicalTIme != other.getTime()){
            return this.logicalTIme - other.getTime() > 0;
        } else {
            return this.writerId.compareTo(other.getWriterId()) > 0;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.hashCode();
        return result;
    }

    //Compare only account numbers
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass()) {
            return false;
        }
        TreasTag other = (TreasTag) obj;
        if (this.getTime() != other.getTime() || !this.getWriterId().equals(other.getWriterId()))
            return false;
        return true;
    }



    public String toString() {
        return logicalTIme + ";" + writerId;
    }
}
