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
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.ODatabaseUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public abstract class OBaseWorkload implements OWorkload {
  public class OWorkLoadContext {
    ODatabase db;
    int       threadId;
    int       currentIdx;
  }

  public class OWorkLoadResult {
    long totalTime;
    long avgNs;
    int  percentileAvg;
    long percentile99Ns;
    long percentile99_9Ns;
  }

  protected static final long MAX_ERRORS = 100;
  protected List<String>      errors     = new ArrayList<String>();

  protected ODatabase getDocumentDatabase(final ODatabaseIdentifier databaseIdentifier) {
    // opens the newly created db and creates an index on the class we're going to use
    final ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier);
    if (database == null)
      throw new IllegalArgumentException("Error on opening database " + databaseIdentifier.getName());

    return database;
  }

  protected OWorkLoadResult executeOperation(final ODatabaseIdentifier dbIdentifier, final int operationTotal,
      final int concurrencyLevel, final OCallable<Void, OWorkLoadContext> callback) {
    final OWorkLoadResult result = new OWorkLoadResult();

    if (operationTotal == 0)
      return result;

    final int totalPerThread = operationTotal / concurrencyLevel;
    final int totalPerLastThread = totalPerThread + operationTotal % concurrencyLevel;

    final ArrayList<Long> operationTiming = new ArrayList<Long>(operationTotal);
    for (int i = 0; i < operationTotal; ++i)
      operationTiming.add(null);

    final Thread[] thread = new Thread[concurrencyLevel];
    for (int t = 0; t < concurrencyLevel; ++t) {
      final OWorkLoadContext context = new OWorkLoadContext();

      context.threadId = t;

      thread[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          final int threadTotal = context.threadId < concurrencyLevel - 1 ? totalPerThread : totalPerLastThread;

          context.db = getDocumentDatabase(dbIdentifier);
          try {
            final int startIdx = totalPerThread * context.threadId;

            for (int i = 0; i < threadTotal; ++i) {
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
            context.db.close();
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

    return result;
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
