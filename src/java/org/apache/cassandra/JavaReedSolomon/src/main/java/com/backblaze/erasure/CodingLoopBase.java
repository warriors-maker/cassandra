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

package org.apache.cassandra.JavaReedSolomon.src.main.java.com.backblaze.erasure;

/**
 * Common implementations for coding loops.
 *
 * Many of the coding loops do not have custom checkSomeShards() methods.
 * The benchmark doesn't measure that method.
 */
public abstract class CodingLoopBase implements CodingLoop
{

    @Override
    public boolean checkSomeShards(
            byte[][] matrixRows,
            byte[][] inputs, int inputCount,
            byte[][] toCheck, int checkCount,
            int offset, int byteCount,
            byte[] tempBuffer) {

        // This is the loop structure for ByteOutputInput, which does not
        // require temporary buffers for checking.
        byte [] [] table = Galois.MULTIPLICATION_TABLE;
        for (int iByte = offset; iByte < offset + byteCount; iByte++) {
            for (int iOutput = 0; iOutput < checkCount; iOutput++) {
                byte [] matrixRow = matrixRows[iOutput];
                int value = 0;
                for (int iInput = 0; iInput < inputCount; iInput++) {
                    value ^= table[matrixRow[iInput] & 0xFF][inputs[iInput][iByte] & 0xFF];
                }
                if (toCheck[iOutput][iByte] != (byte) value) {
                    return false;
                }
            }
        }
        return true;
    }
}
