/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.stresstest.workload;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public abstract class OBaseWorkload implements OWorkload {
  public abstract class OBaseWorkLoadContext {
    public int threadId;
    public int currentIdx;
    public int totalPerThread;

    public abstract void init(ODatabaseIdentifier dbIdentifier);

    public abstract void close();
  }

  public class OWorkLoadResult {
    public AtomicInteger current = new AtomicInteger();
    public int           total   = 1;
    public long          totalTime;
    public long          avgNs;
    public int           percentileAvg;
    public long          percentile99Ns;
    public long          percentile99_9Ns;

    public String toOutput() {
      return String.format("\n- Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th Perc: %.3fms - 99.9th Perc: %.3fms",
          total * 1000 / (float) totalTime, avgNs / 1000000f, percentileAvg, percentile99Ns / 1000000f,
          percentile99_9Ns / 1000000f);
    }

    public ODocument toJSON() {
      final ODocument json = new ODocument();
      json.field("total", total);
      json.field("time", totalTime / 1000f);
      json.field("throughput", totalTime > 0 ? total * 1000 / (float) totalTime : 0);
      json.field("avg", avgNs / 1000000f);
      json.field("percAvg", percentileAvg);
      json.field("perc99", percentile99Ns / 1000000f);
      json.field("perc99_9", percentile99_9Ns / 1000000f);
      return json;
    }
  }

  protected static final long MAX_ERRORS = 100;
  protected List<String>      errors     = new ArrayList<String>();

  protected List<OBaseWorkLoadContext> executeOperation(final ODatabaseIdentifier dbIdentifier, final OWorkLoadResult result,
      final int concurrencyLevel, final OCallable<Void, OBaseWorkLoadContext> callback) {
    if (result.total == 0)
      return null;

    final int totalPerThread = result.total / concurrencyLevel;
    final int totalPerLastThread = totalPerThread + result.total % concurrencyLevel;

    final ArrayList<Long> operationTiming = new ArrayList<Long>(result.total);
    for (int i = 0; i < result.total; ++i)
      operationTiming.add(null);

    final List<OBaseWorkLoadContext> contexts = new ArrayList<OBaseWorkLoadContext>(concurrencyLevel);

    final Thread[] thread = new Thread[concurrencyLevel];
    for (int t = 0; t < concurrencyLevel; ++t) {
      final int currentThread = t;

      final OBaseWorkLoadContext context = getContext();
      contexts.add(context);

      thread[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          context.threadId = currentThread;
          context.totalPerThread = context.threadId < concurrencyLevel - 1 ? totalPerThread : totalPerLastThread;

          context.init(dbIdentifier);
          try {
            final int startIdx = totalPerThread * context.threadId;

            for (int i = 0; i < context.totalPerThread; ++i) {
              context.currentIdx = startIdx + i;

              final long startOp = System.nanoTime();
              try {
                callback.call(context);
              } catch (Exception e) {
                errors.add(e.toString());
                if (errors.size() > MAX_ERRORS) {
                  e.printStackTrace();
                  break;
                }
              } finally {
                operationTiming.set(context.currentIdx, System.nanoTime() - startOp);
              }
            }

          } finally {
            context.close();
          }
        }
      });
    }

    final long startTime = System.currentTimeMillis();

    // START ALL THE THREADS
    for (int t = 0; t < concurrencyLevel; ++t) {
      thread[t].start();
    }

    // WAIT FOR ALL THE THREADS
    for (int t = 0; t < concurrencyLevel; ++t) {
      try {
        thread[t].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // STOP THE COUNTER
    result.totalTime = System.currentTimeMillis() - startTime;

    Collections.sort(operationTiming);

    // COMPUTE THE PERCENTILE
    result.avgNs = (int) (result.totalTime * 1000000 / operationTiming.size());
    result.percentileAvg = getPercentile(operationTiming, result.avgNs);
    result.percentile99Ns = operationTiming.get((int) (operationTiming.size() * 99f / 100f));
    result.percentile99_9Ns = operationTiming.get((int) (operationTiming.size() * 99.9f / 100f));

    return contexts;
  }

  protected abstract OBaseWorkLoadContext getContext();

  protected String getErrors() {
    final StringBuilder buffer = new StringBuilder();
    if (!errors.isEmpty()) {
      buffer.append("\nERRORS:");
      for (int i = 0; i < errors.size(); ++i) {
        buffer.append("\n");
        buffer.append(i);
        buffer.append(": ");
        buffer.append(errors.get(i));
      }
    }
    return buffer.toString();
  }

  protected int getPercentile(final ArrayList<Long> sortedResults, final long time) {
    int j = 0;
    for (; j < sortedResults.size(); j++) {
      final Long valueNs = sortedResults.get(j);
      if (valueNs > time) {
        break;
      }
    }
    return (int) (100 * (j / (float) sortedResults.size()));
  }
}
