/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.etl.loader.OETLLoader;
import com.orientechnologies.orient.etl.transformer.OETLTransformer;
import java.util.List;
import java.util.logging.Level;

/**
 * ETL pipeline: sequence of OETLTransformer and a OETLLoader.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli-at-orientdb.com)
 */
public class OETLPipeline {
  protected final OETLProcessor processor;
  protected final List<OETLTransformer> transformers;
  protected final OETLLoader loader;
  protected final OCommandContext context;
  protected final Level logLevel;
  protected final int maxRetries;
  protected boolean haltOnError;

  protected ODatabasePool pool;

  public OETLPipeline(
      final OETLProcessor processor,
      final List<OETLTransformer> transformers,
      final OETLLoader loader,
      final Level logLevel,
      final int maxRetries,
      final boolean haltOnError) {
    this.processor = processor;
    this.transformers = transformers;
    this.loader = loader;
    this.logLevel = logLevel;
    this.maxRetries = maxRetries;
    this.haltOnError = haltOnError;

    context = new OBasicCommandContext();
  }

  public synchronized void begin() {
    loader.beginLoader(this);
    for (OETLTransformer transformer : transformers) {
      transformer.setContext(context);
      try (ODatabaseDocument db = acquire()) {
        transformer.begin(db);
      }
    }
  }

  public void setPool(ODatabasePool pool) {
    this.pool = pool;
  }

  public OCommandContext getContext() {
    return context;
  }

  protected ODatabaseDocument acquire() {
    if (pool == null) return null;
    ODatabaseDocument db = pool.acquire();
    db.activateOnCurrentThread();
    return db;
  }

  protected Object execute(final OETLExtractedItem source) {
    int retry = 0;
    do {
      try (ODatabaseDocument db = acquire()) {
        try {
          Object current = source.payload;

          context.setVariable("extractedNum", source.num);
          context.setVariable("extractedPayload", source.payload);

          for (OETLTransformer t : transformers) {
            current = t.transform(db, current);
            if (current == null) {
              processor
                  .getContext()
                  .getMessageHandler()
                  .warn(this, "Transformer [%s] returned null, skip rest of pipeline execution", t);
            }
          }
          if (current != null) {
            // LOAD
            loader.load(db, current, context);
          }

          db.commit();
          return current;
        } catch (ONeedRetryException e) {
          loader.rollback(db);
          retry++;
          processor
              .getContext()
              .getMessageHandler()
              .info(
                  this,
                  "Error in pipeline execution, retry = %d/%d (exception=)",
                  retry,
                  maxRetries,
                  e);
        } catch (OETLProcessHaltedException e) {
          processor.getContext().getMessageHandler().error(this, "Pipeline execution halted");

          processor.getStats().incrementErrors();

          loader.rollback(db);
          throw e;

        } catch (Exception e) {
          processor.getContext().getMessageHandler().error(this, "Error in Pipeline execution:", e);

          processor.getStats().incrementErrors();

          if (!haltOnError) {
            return null;
          }

          loader.rollback(db);
          throw OException.wrapException(new OETLProcessHaltedException("Halt"), e);
        }
      }
    } while (retry < maxRetries);

    return this;
  }

  public void end() {
    //    pool.close();
  }
}
