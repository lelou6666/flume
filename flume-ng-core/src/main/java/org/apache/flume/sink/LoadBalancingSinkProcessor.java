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
package org.apache.flume.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.util.SpecificOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>Provides the ability to load-balance flow over multiple sinks.</p>
 *
 * <p>The <tt>LoadBalancingSinkProcessor</tt> maintains an indexed list of
 * active sinks on which the load must be distributed. This implementation
 * supports distributing load using either via <tt>ROUND_ROBIN</tt> or via
 * <tt>RANDOM</tt> selection mechanism. The choice of selection mechanism
 * defaults to <tt>ROUND_ROBIN</tt> type, but can be overridden via
 * configuration.</p>
 *
 * <p>When invoked, this selector picks the next sink using its configured
 * selection mechanism and invokes it. In case the selected sink fails with
 * an exception, the processor picks the next available sink via its configured
 * selection mechanism. This implementation does not blacklist the failing
 * sink and instead continues to optimistically attempt every available sink.
 * If all sinks invocations result in failure, the selector propagates the
 * failure to the sink runner.</p>
 *
 * <p>
 * Sample configuration:
 *  <pre>
 *  host1.sinkgroups.group1.sinks = sink1 sink2
 *  host1.sinkgroups.group1.processor.type = load_balance
 *  host1.sinkgroups.group1.processor.selector = <selector type>
 *  host1.sinkgroups.group1.processor.selector.selector_property = <value>
 *  </pre>
 *
 * The value of processor.selector could be either <tt>round_robin</tt> for
 * round-robin scheme of load-balancing or <tt>random</tt> for random
 * selection. Alternatively you can specify your own implementation of the
 * selection algorithm by implementing the <tt>LoadBalancingSelector</tt>
 * interface. If no selector mechanism is specified, the round-robin selector
 * is used by default.
 * </p>
 * <p>
 * This implementation is not thread safe at this time
 * </p>
 *
 * @see FailoverSinkProcessor
 * @see LoadBalancingSinkProcessor.SinkSelector
 */
public class LoadBalancingSinkProcessor extends AbstractSinkProcessor {
  public static final String CONFIG_SELECTOR = "selector";
  public static final String CONFIG_SELECTOR_PREFIX = CONFIG_SELECTOR + ".";

  public static final String SELECTOR_NAME_ROUND_ROBIN = "ROUND_ROBIN";
  public static final String SELECTOR_NAME_RANDOM = "RANDOM";
  public static final String SELECTOR_NAME_ROUND_ROBIN_BACKOFF = "ROUND_ROBIN_BACKOFF";
  public static final String SELECTOR_NAME_RANDOM_BACKOFF = "RANDOM_BACKOFF";

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LoadBalancingSinkProcessor.class);

  private SinkSelector selector;

  @Override
  public void configure(Context context) {
    Preconditions.checkState(getSinks().size() > 1,
        "The LoadBalancingSinkProcessor cannot be used for a single sink. "
        + "Please configure more than one sinks and try again.");

    String selectorTypeName = context.getString(CONFIG_SELECTOR,
        SELECTOR_NAME_ROUND_ROBIN);

    selector = null;

    if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_ROUND_ROBIN)) {
      selector = new RoundRobinSinkSelector();
    } else if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_RANDOM)) {
      selector = new RandomOrderSinkSelector();
    } else if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_ROUND_ROBIN_BACKOFF)) {
      selector = new BackoffRoundRobinSinkSelector();
    } else if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_RANDOM_BACKOFF)) {
      selector = new BackoffRandomOrderSinkSelector();
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SinkSelector> klass = (Class<? extends SinkSelector>)
            Class.forName(selectorTypeName);

        selector = klass.newInstance();
      } catch (Exception ex) {
        throw new FlumeException("Unable to instantiate sink selector: "
            + selectorTypeName, ex);
      }
    }

    selector.setSinks(getSinks());
    selector.configure(
        new Context(context.getSubProperties(CONFIG_SELECTOR_PREFIX)));

    LOGGER.debug("Sink selector: " + selector + " initialized");
  }

  @Override
  public void start() {
    super.start();

    selector.start();
  }

  @Override
  public void stop() {
    super.stop();

    selector.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Iterator<Sink> sinkIterator = selector.createSinkIterator();
    while (sinkIterator.hasNext()) {
      Sink sink = sinkIterator.next();
      try {
        status = sink.process();
        break;
      } catch (Exception ex) {
        selector.informSinkFailed(sink);
        LOGGER.warn("Sink failed to consume event. "
            + "Attempting next sink if available.", ex);
      }
    }

    if (status == null) {
      throw new EventDeliveryException("All configured sinks have failed");
    }

    return status;
  }


  /**
   * <p>
   * An interface that allows the LoadBalancingSinkProcessor to use
   * a load-balancing strategy such as round-robin, random distribution etc.
   * Implementations of this class can be plugged into the system via
   * processor configuration and are used to select a sink on every invocation.
   * </p>
   * <p>
   * An instance of the configured sink selector is create during the processor
   * configuration, its {@linkplain #setSinks(List)} method is invoked following
   * which it is configured via a subcontext. Once configured, the lifecycle of
   * this selector is tied to the lifecycle of the sink processor.
   * </p>
   * <p>
   * At runtime, the processor invokes the {@link #createSinkIterator()}
   * method for every <tt>process</tt> call to create an iteration order over
   * the available sinks. The processor then loops through this iteration order
   * until one of the sinks succeeds in processing the event. If the iterator
   * is exhausted and none of the sinks succeed, the processor will raise
   * an <tt>EventDeliveryException</tt>.
   * </p>
   */
  public interface SinkSelector extends Configurable, LifecycleAware {

    void setSinks(List<Sink> sinks);

    Iterator<Sink> createSinkIterator();

    void informSinkFailed(Sink failedSink);
  }

  /**
   * A sink selector that implements the round-robin sink selection policy.
   * This implementation is not MT safe.
   */
  private static class RoundRobinSinkSelector extends AbstractSinkSelector {

    private int nextHead = 0;

    @Override
    public Iterator<Sink> createSinkIterator() {

      int size = getSinks().size();
      int[] indexOrder = new int[size];

      int begin = nextHead++;
      if (nextHead == size) {
        nextHead = 0;
      }

      for (int i=0; i < size; i++) {
        indexOrder[i] = (begin + i)%size;
      }

      return new SpecificOrderIterator<Sink>(indexOrder, getSinks());
    }
  }

  /**
   * A sink selector that implements a random sink selection policy. This
   * implementation is not thread safe.
   */
  private static class RandomOrderSinkSelector extends AbstractSinkSelector {

    private Random random = new Random(System.currentTimeMillis());

    @Override
    public Iterator<Sink> createSinkIterator() {
      int size = getSinks().size();
      int[] indexOrder = new int[size];

      List<Integer> indexList = new ArrayList<Integer>();
      for (int i=0; i<size; i++) {
        indexList.add(i);
      }

      while (indexList.size() != 1) {
        int pick = random.nextInt(indexList.size());
        indexOrder[indexList.size() - 1] = indexList.remove(pick);
      }

      indexOrder[0] = indexList.get(0);

      return new SpecificOrderIterator<Sink>(indexOrder, getSinks());
    }
  }

  private static class FailureState {
    long lastFail;
    long restoreTime;
    int sequentialFails;
  }

  public static abstract class AbstractBackoffSinkSelector extends AbstractSinkSelector {
    // 2 ^ 16 seconds should be more than enough for an upper limit...
    private static final int EXP_BACKOFF_COUNTER_LIMIT = 16;
    private static final String CONF_MAX_TIMEOUT = "maxBackoffMillis";
    private static final long CONSIDER_SEQUENTIAL_RANGE = 2000l;
    private static final long MAX_TIMEOUT = 30000l;

    protected List<FailureState> sinkStates;
    protected Map<Sink, FailureState> stateMap;
    protected  long maxTimeout = MAX_TIMEOUT;

    @Override
    public void configure(Context context) {
      super.configure(context);
      maxTimeout = context.getLong(CONF_MAX_TIMEOUT, MAX_TIMEOUT);
    }

    @Override
    public void setSinks(List<Sink> sinks) {
      super.setSinks(sinks);
      sinkStates = new ArrayList<FailureState>();
      stateMap = new HashMap<Sink, FailureState>();
      for(Sink sink : sinks) {
        FailureState state = new FailureState();
        sinkStates.add(state);
        stateMap.put(sink, state);
      }
    }

    @Override
    public void informSinkFailed(Sink failedSink) {
      super.informSinkFailed(failedSink);
      FailureState state = stateMap.get(failedSink);
      long now = System.currentTimeMillis();
      long delta = now - state.lastFail;

      long lastBackoffLength = Math.min(MAX_TIMEOUT, 1000 * (1 << state.sequentialFails));
      long allowableDiff = lastBackoffLength + CONSIDER_SEQUENTIAL_RANGE;
      if( allowableDiff > delta ) {
        if(state.sequentialFails < EXP_BACKOFF_COUNTER_LIMIT)
        state.sequentialFails++;
      } else {
        state.sequentialFails = 1;
      }
      state.lastFail = now;
      state.restoreTime = now + Math.min(MAX_TIMEOUT, 1000 * (1 << state.sequentialFails));
    }

  }


  private static class BackoffRoundRobinSinkSelector extends AbstractBackoffSinkSelector {
    private int nextHead = 0;

    @Override
    public Iterator<Sink> createSinkIterator() {
      long curTime = System.currentTimeMillis();
      List<Integer> activeIndices = new ArrayList<Integer>();
      int index = 0;
      for(FailureState state : sinkStates) {
        if (state.restoreTime < curTime) {
          activeIndices.add(index);
        }
        index++;
      }

      int size = activeIndices.size();
      // possible that the size has shrunk so gotta adjust nextHead for that
      if(nextHead >= size) {
        nextHead = 0;
      }
      int begin = nextHead++;
      if (nextHead == activeIndices.size()) {
        nextHead = 0;
      }

      int[] indexOrder = new int[size];

      for (int i=0; i < size; i++) {
        indexOrder[i] = activeIndices.get((begin + i) % size);
      }

      return new SpecificOrderIterator<Sink>(indexOrder, getSinks());
    }
  }

  /**
   * A sink selector that implements a random sink selection policy. This
   * implementation is not thread safe.
   */
  private static class BackoffRandomOrderSinkSelector extends AbstractBackoffSinkSelector {
    private Random random = new Random(System.currentTimeMillis());

    @Override
    public Iterator<Sink> createSinkIterator() {
      long now = System.currentTimeMillis();

      List<Integer> indexList = new ArrayList<Integer>();

      int i = 0;
      for (FailureState state : sinkStates) {
        if(state.restoreTime < now)
          indexList.add(i);
        i++;
      }

      int size = indexList.size();
      int[] indexOrder = new int[size];

      while (indexList.size() != 1) {
        int pick = random.nextInt(indexList.size());
        indexOrder[indexList.size() - 1] = indexList.remove(pick);
      }

      indexOrder[0] = indexList.get(0);

      return new SpecificOrderIterator<Sink>(indexOrder, getSinks());
    }
  }

}
