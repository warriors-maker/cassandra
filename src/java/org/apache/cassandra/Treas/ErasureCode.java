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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.JavaReedSolomon.src.main.java.com.backblaze.erasure.ReedSolomon;
import org.apache.cassandra.service.StorageProxy;
import org.mortbay.util.IO;

public class ErasureCode
{

    public static final int DATA_SHARDS = TreasConfig.num_recover;
    public static final int PARITY_SHARDS = TreasConfig.num_server - TreasConfig.num_recover;
    public static final int TOTAL_SHARDS = TreasConfig.num_server;
    private static final Logger logger = LoggerFactory.getLogger(ErasureCode.class);

    public static final int BYTES_IN_INT = 4;

//    public static ReedSolomon getReedSolomon () {
//        if (reedSolomon == null) {
//            reedSolomon = ReedSolomon.create(TreasConfig.num_recover, TreasConfig.num_server - TreasConfig.num_recover);
//        }
//        return reedSolomon;
//    }

    public byte[][] encodeData(String value) {
        //logger.debug("The value is" + value);
        ReedSolomon reedSolomon = ReedSolomon.create(TreasConfig.num_recover, TreasConfig.num_server - TreasConfig.num_recover);
        final int valueSize =  value.length();
        //logger.debug("Inside encodeData");

        // Figure out how big each shard will be.  The total size stored
        // will be the file size (8 bytes) plus the file.
        final int storedSize = valueSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;


        // Create a buffer holding the file size, followed by
        // the contents of the file.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte [] allBytes = new byte[bufferSize];


        ByteBuffer.wrap(allBytes).putInt(valueSize);
        InputStream in = new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));

        try {
            in.read(allBytes, BYTES_IN_INT, valueSize);
        } catch (IOException e) {
            e.printStackTrace();
        } finally
        {
            try
            {
                in.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        // Make the buffers to hold the shards.
        byte [] [] shards = new byte [TOTAL_SHARDS] [shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        //logger.debug("Before Encode Parity");
        reedSolomon.encodeParity(shards, 0, shardSize);
//        logger.debug("Coding looks like the folliowing");
//        for (int i = 0; i < shards.length; i++) {
//            logger.debug(TreasConfig.byteToString(shards[i]));
//        }
        return shards;
    }

    public String decodeeData(byte[][] shards, boolean []shardPresent, int shardSize, String key) {
        ReedSolomon reedSolomon = ReedSolomon.create(TreasConfig.num_recover, TreasConfig.num_server - TreasConfig.num_recover);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        byte [] decodeBytes = new byte[shardSize * DATA_SHARDS];

        //System.out.println("valueSize is" + shardSize);
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, decodeBytes, shardSize * i, shardSize);
        }

        int valueSize = ByteBuffer.wrap(decodeBytes).getInt();

        OutputStream out = new ByteArrayOutputStream();

        try
        {
            logger.debug(decodeBytes.length + " " + valueSize + " " + key);
            out.write(decodeBytes, BYTES_IN_INT, valueSize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return out.toString();
    }
}