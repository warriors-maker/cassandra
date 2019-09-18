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
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageProxy;

public class TreasUtil
{
    private static final Logger logger = LoggerFactory.getLogger(TreasUtil.class);
    public synchronized static Long getLong(ByteBuffer bb) {
//        int pos = -1;
//        Long time = null;
//        try {
//
//        } catch (Exception e) {
//
//            throw e;
//        }
        int pos = bb.position();
        Long time = bb.getLong();
        bb.position(pos);
        return time;
    }

//    public synchronized static void printTags(HashMap<Long,List<String>> map ,HashMap<Long, Integer> decodeCountMap) {
//        for (Long tag: map.keySet()) {
//            logger.debug(tag + " " + decodeCountMap.get(tag));
//        }
//    }
}
