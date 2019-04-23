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
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HandlerReadThread extends Thread
{
    private PriorityBlockingQueue<PQObject> priorityBlockingQueue;
    private TimeVector timeVector;
    private static final Logger logger = LoggerFactory.getLogger(HandlerReadThread.class);

    public HandlerReadThread(CausalObject causalObject) {
        this.priorityBlockingQueue = causalObject.getPriorityBlockingQueue();
        this.timeVector = causalObject.getTimeVector();
    }

    private void batchCommit() {
        // Fetch the current timeStamp;
        while (true)
        {
            try
            {
                logger.debug("Size is" + priorityBlockingQueue.size());

                PQObject pqObject = this.priorityBlockingQueue.take();
                logger.debug("The head of pq timeStamp is:");
                printMutation(pqObject);

                List<Integer> localTimeVector = timeVector.read();
                logger.debug("My timeStamp is");
                CausalCommon.getInstance().printTimeStamp(localTimeVector);

                if (CausalCommon.getInstance().canCommit(localTimeVector, pqObject.getMutationTimeStamp(), pqObject.getSenderID()))
                {
                    timeVector.updateAndRead(pqObject.getSenderID());
                    CausalCommon.getInstance().commit(pqObject.getMutation());
                } else {
                    logger.debug("Fail timeStamp:");
                    this.priorityBlockingQueue.offer(pqObject);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void printMutation(PQObject pqObject) {
        if (pqObject == null) {
            logger.debug("Null");
        } else {
            CausalCommon.getInstance().printTimeStamp(pqObject.getMutationTimeStamp());
        }
    }

    public void printTimeStamp(List<Integer> timeStamp) {
        StringBuilder sb = new StringBuilder();
        sb.append("pq: ");
        for (int i = 0; i < timeStamp.size(); i++) {
            sb.append (timeStamp.get(i) + ",");
        }
//        logger.debug(sb.toString());
    }

    @Override
    public void run()
    {
        batchCommit();
    }
}
