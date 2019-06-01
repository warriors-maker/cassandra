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

public class ErasureCode
{
    public native void create_encode_decode_matrix(int k, int p);
    public native void destroy_encode_decode_matrix();
    public native void cmain(byte[] data);
    public native byte[][] encode_data(byte[] data);
    public native byte[][] decode_data(byte[][] encoded_data, int []erased_indices);

    static
    {
        try
        {
            System.loadLibrary( "/root/JavaISal/javaexample/liberasure.so" );
        }
        catch ( Exception e )
        {
            e.printStackTrace( );
        }
    }

}
