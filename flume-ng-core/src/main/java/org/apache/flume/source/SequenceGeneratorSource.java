/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source;

import java.util.ArrayList;
import java.util.List;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
<<<<<<< HEAD
import org.apache.flume.PollableSource;
=======
import org.apache.flume.FlumeException;
>>>>>>> refs/remotes/apache/trunk
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
public class SequenceGeneratorSource extends AbstractSource implements
    PollableSource, Configurable {
=======
public class SequenceGeneratorSource extends AbstractPollableSource implements
        Configurable {
>>>>>>> refs/remotes/apache/trunk

  private static final Logger logger = LoggerFactory
      .getLogger(SequenceGeneratorSource.class);

  private long sequence;
  private int batchSize;
  private SourceCounter sourceCounter;
  private List<Event> batchArrayList;
  private long totalEvents;
  private long eventsSent = 0;

  public SequenceGeneratorSource() {
    sequence = 0;
<<<<<<< HEAD
  }

  /**
   * Read parameters from context
   * <li>batchSize = type int that defines the size of event batches
   */
  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", 1);
    if (batchSize > 1) {
      batchArrayList = new ArrayList<Event>(batchSize);
    }
    totalEvents = context.getLong("totalEvents", Long.MAX_VALUE);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
=======
>>>>>>> refs/remotes/apache/trunk
  }

  /**
   * Read parameters from context
   * <li>batchSize = type int that defines the size of event batches
   */
  @Override
  protected void doConfigure(Context context) throws FlumeException {
    batchSize = context.getInteger("batchSize", 1);
    if (batchSize > 1) {
      batchArrayList = new ArrayList<Event>(batchSize);
    }
    totalEvents = context.getLong("totalEvents", Long.MAX_VALUE);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

<<<<<<< HEAD
=======
  @Override
  protected Status doProcess() throws EventDeliveryException {
>>>>>>> refs/remotes/apache/trunk
    Status status = Status.READY;
    int i = 0;
    try {
      if (batchSize <= 1) {
        if(eventsSent < totalEvents) {
          getChannelProcessor().processEvent(
<<<<<<< HEAD
            EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
=======
                  EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
>>>>>>> refs/remotes/apache/trunk
          sourceCounter.incrementEventAcceptedCount();
          eventsSent++;
        } else {
          status = Status.BACKOFF;
        }
      } else {
        batchArrayList.clear();
        for (i = 0; i < batchSize; i++) {
          if(eventsSent < totalEvents){
            batchArrayList.add(i, EventBuilder.withBody(String
<<<<<<< HEAD
              .valueOf(sequence++).getBytes()));
=======
                    .valueOf(sequence++).getBytes()));
>>>>>>> refs/remotes/apache/trunk
            eventsSent++;
          } else {
            status = Status.BACKOFF;
          }
        }
        if(!batchArrayList.isEmpty()) {
          getChannelProcessor().processEventBatch(batchArrayList);
          sourceCounter.incrementAppendBatchAcceptedCount();
          sourceCounter.addToEventAcceptedCount(batchArrayList.size());
        }
      }

    } catch (ChannelException ex) {
      eventsSent -= i;
      logger.error( getName() + " source could not write to channel.", ex);
    }

    return status;
  }

  @Override
<<<<<<< HEAD
  public void start() {
    logger.info("Sequence generator source starting");

    super.start();
    sourceCounter.start();
    logger.debug("Sequence generator source started");
=======
  protected void doStart() throws FlumeException {
    logger.info("Sequence generator source do starting");
    sourceCounter.start();
    logger.debug("Sequence generator source do started");
>>>>>>> refs/remotes/apache/trunk
  }

  @Override
  protected void doStop() throws FlumeException {
    logger.info("Sequence generator source do stopping");

<<<<<<< HEAD
    super.stop();
    sourceCounter.stop();

    logger.info("Sequence generator source stopped. Metrics:{}",getName(), sourceCounter);
=======
    sourceCounter.stop();

    logger.info("Sequence generator source do stopped. Metrics:{}",getName(), sourceCounter);
>>>>>>> refs/remotes/apache/trunk
  }

}
