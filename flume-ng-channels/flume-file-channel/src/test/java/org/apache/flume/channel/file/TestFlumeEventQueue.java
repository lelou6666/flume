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
package org.apache.flume.channel.file;

import com.google.common.collect.SetMultimap;
import java.io.File;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestFlumeEventQueue {

  File file;
  File inflightTakes;
  File inflightPuts;
  FlumeEventPointer pointer1 = new FlumeEventPointer(1, 1);
  FlumeEventPointer pointer2 = new FlumeEventPointer(2, 2);
  FlumeEventQueue queue;
  @Before
  public void setup() throws Exception {
    file = File.createTempFile("Checkpoint", "");
    inflightTakes = File.createTempFile("inflighttakes", "");
    inflightPuts = File.createTempFile("inflightputs", "");
  }
  @Test
  public void testQueueIsEmptyAfterCreation() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertNull(queue.removeHead(0));
  }
  @Test
  public void testCapacity() throws Exception {
    queue = new FlumeEventQueue(1, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertFalse(queue.addTail(pointer2));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityZero() throws Exception {
    queue = new FlumeEventQueue(0, file, inflightTakes, inflightPuts,"test");
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityNegative() throws Exception {
    queue = new FlumeEventQueue(-1, file, inflightTakes, inflightPuts,"test");
  }
  @Test
  public void addTail1() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTail2() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(2), queue.getFileIDs());
  }
  @Test
  public void addTailLarge() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addTail(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = 1; i <= size; i++) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead(0));
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead1() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead2() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer2, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
  }
  @Test
  public void addHeadLarge() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addHead(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = size; i > 0; i--) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead(0));
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTailRemove1() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
    Assert.assertNull(queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }

  @Test
  public void addTailRemove2() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(pointer2, queue.removeHead(0));
  }

  @Test
  public void addHeadRemove1() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    queue.addHead(pointer1);
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertNull(queue.removeHead(0));
  }
  @Test
  public void addHeadRemove2() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(pointer2, queue.removeHead(0));
  }
  @Test
  public void testWrappingCorrectly() throws Exception {
    queue = new FlumeEventQueue(1000, file, inflightTakes, inflightPuts,"test");
    int size = Integer.MAX_VALUE;
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
    for (int i = queue.getSize()/2; i > 0; i--) {
      Assert.assertNotNull(queue.removeHead(0));
    }
    // addHead below would throw an IndexOOBounds with
    // bad version of FlumeEventQueue.convert
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
  }
  @Test
  public void testInflightPuts() throws Exception{
    queue = new FlumeEventQueue(10, file, inflightTakes, inflightPuts, "test");
    long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
    long txnID2 = txnID1 + 1;
    queue.addWithoutCommit(new FlumeEventPointer(1, 1), txnID1);
    queue.addWithoutCommit(new FlumeEventPointer(2, 1), txnID1);
    queue.addWithoutCommit(new FlumeEventPointer(2, 2), txnID2);
    queue.checkpoint(true);
    TimeUnit.SECONDS.sleep(3L);
    queue = new FlumeEventQueue(10, file, inflightTakes, inflightPuts, "test");
    SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightPuts();
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(2, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 2).toLong()));
  }

  @Test
  public void testInflightTakes() throws Exception {
    queue = new FlumeEventQueue(10, file, inflightTakes, inflightPuts, "test");
    long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
    long txnID2 = txnID1 + 1;
    queue.addTail(new FlumeEventPointer(1, 1));
    queue.addTail(new FlumeEventPointer(2, 1));
    queue.addTail(new FlumeEventPointer(2, 2));
    queue.removeHead(txnID1);
    queue.removeHead(txnID2);
    queue.removeHead(txnID2);
    queue.checkpoint(true);
    TimeUnit.SECONDS.sleep(3L);
    queue = new FlumeEventQueue(10, file, inflightTakes, inflightPuts, "test");
        SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightTakes();
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 2).toLong()));

  }
}

