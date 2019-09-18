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

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConfig
{
    public final static int num_server = 5;
    public final static int num_intersect = 4;
    public final static int num_recover = 2;
    public final static int num_concurrecy = 3;

    public static final String TAG_ONE  = "tag1";
    public static final String TAG_TWO  = "tag2";
    public static final String TAG_THREE  = "tag3";

    public static final String VAL_ONE =  "field1";
    public static final String VAL_TWO =  "field2";
    public static final String VAL_THREE =  "field3";

    public static final String VAL_PREFIX =  "field";
    public static final String TAG_PREFIX =  "tag";

    //public static final String[] ADDRESSES = {"localhost/127.0.0.1"};

    //public static final String[] ADDRESSES = {"10.142.15.199", "10.142.15.200","10.142.15.201","10.142.15.202","10.142.15.203"};

    public static final String[] ADDRESSES = {"10.0.0.1", "10.0.0.2","10.0.0.3","10.0.0.4","10.0.0.5"};

    private static HashMap<String, Integer> map = new HashMap<>();

    public static final int QUORUM = (int) Math.ceil ( (TreasConfig.num_server + TreasConfig.num_intersect) / 2);


    // Convert the byte array to String to send back to client
    public static String byteToString(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // Convert incoming String value
    public static byte[] stringToByte(String value) {
        return value.getBytes(Charset.forName("UTF-8"));
    }

    // Create the Empty Codes based on what I set
    public static byte[] emptyCodes(int length) {
        byte[] arr = new byte[length];
        for (int i = 0; i < length; i++) {
            arr[i] = '0';
        }
        return arr;
    }

    public static void initiateAddressMap() {
        for (int i = 0; i < ADDRESSES.length; i++) {
            map.put(ADDRESSES[i], i);
        }
    }

    public static HashMap<String, Integer> getAddressMap() {
        return map;
    }

}
