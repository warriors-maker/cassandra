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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;

public class StorageProxyWrite
{


    private Object obj1 = new Object();
    private Object obj2 = new Object();
    private Object obj3 = new Object();
    private Object obj4 = new Object();

    private static StorageProxyWrite sw = new StorageProxyWrite();

    public static StorageProxyWrite getLogTime() {
        return sw;
    }

    private final String absPath = "/root/cassandra/logs/";

    private  void initFile(String name){
        File file = new File(name);
        try
        {
            file.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void writeReadValue(long num) {
        synchronized (obj1) {
            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
            String name = absPath + "ReadValue" + (index + 1) + ".txt";
            FileWriter writer = null;
            try
            {
                initFile(name);
                writer = new FileWriter(name,true);
            } catch  (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter printWriter = new BufferedWriter (writer);
            try {
                printWriter.write(num+"");
                printWriter.newLine();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeReadTag(long num) {
        synchronized (obj2) {
            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
            String name = absPath + "WriteReadTag" + (index + 1) + ".txt";
            FileWriter writer = null;
            try
            {
                initFile(name);
                writer = new FileWriter(name,true);
            } catch  (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter printWriter = new BufferedWriter (writer);
            try {
                printWriter.write(num+"");
                printWriter.newLine();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeMutationMain(long num) {
        synchronized (obj3) {
            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
            String name = absPath + "writeMutationMain" + (index + 1) + ".txt";
            FileWriter writer = null;
            try
            {
                initFile(name);
                writer = new FileWriter(name,true);
            } catch  (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter printWriter = new BufferedWriter (writer);
            try {
                printWriter.write(num+"");
                printWriter.newLine();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public void writeMutationVerb(long num) {
        synchronized (obj4) {
            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
            String name = absPath + "MutationVerb" + (index + 1) + ".txt";
            FileWriter writer = null;
            try
            {
                initFile(name);
                writer = new FileWriter(name,true);
            } catch  (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter printWriter = new BufferedWriter (writer);
            try {
                printWriter.write(num+"");
                printWriter.newLine();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
