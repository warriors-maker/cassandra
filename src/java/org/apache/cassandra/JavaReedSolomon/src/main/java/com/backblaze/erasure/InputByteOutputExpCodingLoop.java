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

public class InputByteOutputExpCodingLoop extends CodingLoopBase {

    @Override
    public void codeSomeShards(
            byte[][] matrixRows,
            byte[][] inputs, int inputCount,
            byte[][] outputs, int outputCount,
            int offset, int byteCount) {

        {
            final int iInput = 0;
            final byte[] inputShard = inputs[iInput];
            for (int iByte = offset; iByte < offset + byteCount; iByte++) {
                final byte inputByte = inputShard[iByte];
                for (int iOutput = 0; iOutput < outputCount; iOutput++) {
                    final byte[] outputShard = outputs[iOutput];
                    final byte[] matrixRow = matrixRows[iOutput];
                    outputShard[iByte] = Galois.multiply(matrixRow[iInput], inputByte);
                }
            }
        }

        for (int iInput = 1; iInput < inputCount; iInput++) {
            final byte[] inputShard = inputs[iInput];
            for (int iByte = offset; iByte < offset + byteCount; iByte++) {
                final byte inputByte = inputShard[iByte];
                for (int iOutput = 0; iOutput < outputCount; iOutput++) {
                    final byte[] outputShard = outputs[iOutput];
                    final byte[] matrixRow = matrixRows[iOutput];
                    outputShard[iByte] ^= Galois.multiply(matrixRow[iInput], inputByte);
                }
            }
        }
    }

}
