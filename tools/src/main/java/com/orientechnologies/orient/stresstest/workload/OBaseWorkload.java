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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public abstract class OBaseWorkload implements OWorkload {
  protected OStorageRemote.CONNECTION_STRATEGY connectionStrategy = OStorageRemote.CONNECTION_STRATEGY.STICKY;

  public abstract class OBaseWorkLoadContext {
    public int threadId;
    public int currentIdx;
    public int totalPerThread;

    public abstract void init(ODatabaseIdentifier dbIdentifier, int operationsPerTransaction);

    public abstract void close();
  }

  public class OWorkLoadResult {
    public AtomicInteger current = new AtomicInteger();
    public int           total   = 1;
    public long totalTime;
    public long totalTimeOperationsNs;
    public long throughputAvgNs;

    public long latencyAvgNs;
    public long latencyMinNs;
    public long latencyMaxNs;
    public int  latencyPercentileAvg;
    public long latencyPercentile99Ns;
    public long latencyPercentile99_9Ns;

    public AtomicInteger conflicts = new AtomicInteger();

    public String toOutput(final int leftSpaces) {
      final StringBuilder indent = new StringBuilder();
      for (int i = 0; i < leftSpaces; ++i)
        indent.append(' ');

      return String.format(
          "\n%s- Throughput: %.3f/sec (Avg %.3fms/op)\n%s- Latency Avg: %.3fms/op (%dth percentile) - Min: %.3fms - 99th Perc: %.3fms - 99.9th Perc: %.3fms - Max: %.3fms - Conflicts: %d",
          indent, total * 1000 / (float) totalTime, throughputAvgNs / 1000000f, indent, latencyAvgNs / 1000000f,
          latencyPercentileAvg, latencyMinNs / 1000000f, latencyPercentile99Ns / 1000000f, latencyPercentile99_9Ns / 1000000f,
          latencyMaxNs / 1000000f, conflicts.get());
    }

    public ODocument toJSON() {
      final ODocument json = new ODocument();
      json.field("total", total);
      json.field("time", totalTime / 1000f);
      json.field("timeOperations", totalTimeOperationsNs / 1000f);

      json.field("throughput", totalTime > 0 ? total * 1000 / (float) totalTime : 0);
      json.field("throughputAvg", throughputAvgNs / 1000000f);

      json.field("latencyAvg", latencyAvgNs / 1000000f);
      json.field("latencyMin", latencyMinNs / 1000000f);
      json.field("latencyPercAvg", latencyPercentileAvg);
      json.field("latencyPerc99", latencyPercentile99Ns / 1000000f);
      json.field("latencyPerc99_9", latencyPercentile99_9Ns / 1000000f);
      json.field("latencyMax", latencyMaxNs / 1000000f);
      json.field("conflicts", conflicts.get());
      return json;
    }
  }

  protected static final long         MAX_ERRORS = 100;
  protected              List<String> errors     = new ArrayList<String>();

  protected List<OBaseWorkLoadContext> executeOperation(final ODatabaseIdentifier dbIdentifier, final OWorkLoadResult result,
      final OStressTesterSettings settings, final OCallable<Void, OBaseWorkLoadContext> callback) {

    if (result.total == 0)
      return null;

    final int concurrencyLevel = settings.concurrencyLevel;
    final int operationsPerTransaction = settings.operationsPerTransaction;

    final int totalPerThread = result.total / concurrencyLevel;
    final int totalPerLastThread = totalPerThread + result.total % concurrencyLevel;

    final Long[] operationTiming = new Long[result.total];

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

          context.init(dbIdentifier, operationsPerTransaction);

          init(context);

          try {
            final int startIdx = totalPerThread * context.threadId;

            final AtomicInteger operationsExecutedInTx = new AtomicInteger();

            for (final AtomicInteger i = new AtomicInteger(); i.get() < context.totalPerThread; i.incrementAndGet()) {
              ODatabaseDocumentTx.executeWithRetries(new OCallable<Object, Integer>() {
                @Override
                public Object call(final Integer retry) {
                  if (retry > 0) {
                    i.addAndGet(operationsExecutedInTx.get() * -1);
                    if (i.get() < 0)
                      i.set(0);
                    operationsExecutedInTx.set(0);
                  }

                  context.currentIdx = startIdx + i.get();

                  final long startOp = System.nanoTime();
                  try {

                    try {
                      return callback.call(context);
                    } finally {
                      operationsExecutedInTx.incrementAndGet();

                      if (operationsPerTransaction > 0 && (i.get() + 1) % operationsPerTransaction == 0
                          || i.get() == context.totalPerThread - 1) {
                        commitTransaction(context);
                        operationsExecutedInTx.set(0);
                        beginTransaction(context);
                      }
                    }

                  } catch (ONeedRetryException e) {
                    result.conflicts.incrementAndGet();

                    manageNeedRetryException(context, e);

                    if (operationsPerTransaction > 0)
                      beginTransaction(context);

                    throw e;

                  } catch (Exception e) {
                    errors.add(e.toString());
                    if (errors.size() > MAX_ERRORS) {
                      e.printStackTrace();
                      return null;
                    }
                  } finally {
                    operationTiming[context.currentIdx] = System.nanoTime() - startOp;
                  }

                  return null;
                }
              }, 10);

              if (settings.delay > 0)
                try {
                  Thread.sleep(settings.delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
            }

            if (operationsPerTransaction > 0)
              commitTransaction(context);

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

    Arrays.sort(operationTiming);

    result.throughputAvgNs = (int) (result.totalTime * 1000000 / operationTiming.length);

    // COMPUTE THE TOTAL COST OF OPERATIONS ONLY
    result.totalTimeOperationsNs = 0;
    for (long l : operationTiming)
      result.totalTimeOperationsNs += l;

    result.latencyMinNs = operationTiming[0];
    result.latencyMaxNs = operationTiming[operationTiming.length - 1];

    result.latencyAvgNs = (int) (result.totalTimeOperationsNs / operationTiming.length);
    result.latencyPercentileAvg = getPercentile(operationTiming, result.latencyAvgNs);
    result.latencyPercentile99Ns = operationTiming[(int) (operationTiming.length * 99f / 100f)];
    result.latencyPercentile99_9Ns = operationTiming[(int) (operationTiming.length * 99.9f / 100f)];

    return contexts;
  }

  protected abstract void beginTransaction(OBaseWorkLoadContext context);

  protected abstract void commitTransaction(OBaseWorkLoadContext context);

  protected abstract OBaseWorkLoadContext getContext();

  protected void init(OBaseWorkLoadContext context) {
  }

  protected void manageNeedRetryException(final OBaseWorkLoadContext context, final ONeedRetryException e) {
  }

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

  protected int getPercentile(final Long[] sortedResults, final long time) {
    int j = 0;
    for (; j < sortedResults.length; j++) {
      final Long valueNs = sortedResults[j];
      if (valueNs > time) {
        break;
      }
    }
    return (int) (100 * (j / (float) sortedResults.length));
  }
}
