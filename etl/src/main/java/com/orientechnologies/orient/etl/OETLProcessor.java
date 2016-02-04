/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.block.OBlock;
import com.orientechnologies.orient.etl.extractor.OExtractor;
import com.orientechnologies.orient.etl.loader.OLoader;
import com.orientechnologies.orient.etl.source.OSource;
import com.orientechnologies.orient.etl.transformer.OTransformer;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * ETL processor class.
 *
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public class OETLProcessor {
  protected final OETLComponentFactory factory     = new OETLComponentFactory();
  protected List<OBlock>               beginBlocks;
  protected List<OBlock>               endBlocks;
  protected OSource                    source;
  protected OExtractor                 extractor;
  protected OLoader                    loader;
  protected List<OTransformer>         transformers;
  protected OCommandContext            context;
  protected long                       startTime;
  protected long                       elapsed;
  protected OETLProcessorStats         stats       = new OETLProcessorStats();
  protected TimerTask                  dumpTask;
  protected LOG_LEVELS                 logLevel    = LOG_LEVELS.INFO;
  protected boolean                    haltOnError = true;
  protected boolean                    parallel    = false;
  protected int                        maxRetries  = 10;
  private Thread[]                     threads;

  /**
   * Creates an ETL processor by setting all the components on construction.
   *
   * @param iBeginBlocks
   *          List of Blocks to execute at the beginning of processing
   * @param iSource
   *          Source component
   * @param iExtractor
   *          Extractor component
   * @param iTransformers
   *          List of Transformers
   * @param iLoader
   *          Loader component
   * @param iEndBlocks
   *          List of Blocks to execute at the end of processing
   * @param iContext
   *          Execution Context
   */
  public OETLProcessor(final List<OBlock> iBeginBlocks, final OSource iSource, final OExtractor iExtractor,
      final List<OTransformer> iTransformers, final OLoader iLoader, final List<OBlock> iEndBlocks,
      final OCommandContext iContext) {
    beginBlocks = iBeginBlocks;
    source = iSource;
    extractor = iExtractor;
    transformers = iTransformers;
    loader = iLoader;
    endBlocks = iEndBlocks;
    context = iContext;
    init();
  }

  public OETLProcessor() {
  }

  public static void main(final String[] args) {

    System.out.println("OrientDB etl v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    if (args.length == 0) {
      System.out.println("Syntax error, missing configuration file.");
      System.out.println("Use: oetl.sh <json-file>");
      System.exit(1);
    }

    final OETLProcessor processor = parseConfigAndParameters(args);

    processor.execute();
  }

  protected static OETLProcessor parseConfigAndParameters(String[] args) {
    final OCommandContext context = createDefaultContext();

    ODocument configuration = new ODocument().fromJSON("{}");
    for (final String arg : args) {
      if (arg.charAt(0) != '-') {
        try {
          final String config = OIOUtils.readFileAsString(new File(arg));

          configuration.merge(new ODocument().fromJSON(config, "noMap"), true, true);
          // configuration = ;
          ODocument cfgGlobal = configuration.field("config");
          if (cfgGlobal != null) {
            for (String f : cfgGlobal.fieldNames()) {
              context.setVariable(f, cfgGlobal.field(f));
            }
          }
        } catch (IOException e) {
          throw new OConfigurationException("Error on loading config file: " + arg, e);
        }
      }
    }

    // override with args passes by command line
    for (final String arg : args) {
      if (arg.charAt(0) == '-') {
        final String[] parts = arg.substring(1).split("=");
        context.setVariable(parts[0], parts[1]);
      }
    }

    return new OETLProcessor().parse(configuration, context);
  }

  protected static OCommandContext createDefaultContext() {
    final OCommandContext context = new OBasicCommandContext();
    context.setVariable("dumpEveryMs", 1000);
    return context;
  }

  public OETLProcessor parse(final ODocument cfg, final OCommandContext iContext) {
    return parse((Collection<ODocument>) cfg.field("begin"), (ODocument) cfg.field("source"), (ODocument) cfg.field("extractor"),
        (Collection<ODocument>) cfg.field("transformers"), (ODocument) cfg.field("loader"),
        (Collection<ODocument>) cfg.field("end"), iContext);
  }

  /**
   * Creates an ETL processor by setting the configuration of each component.
   *
   * @param iBeginBlocks
   *          List of Block configurations to execute at the beginning of processing
   * @param iSource
   *          Source component configuration
   * @param iExtractor
   *          Extractor component configuration
   * @param iTransformers
   *          List of Transformer configurations
   * @param iLoader
   *          Loader component configuration
   * @param iEndBlocks
   *          List of Block configurations to execute at the end of processing
   * @param iContext
   *          Execution Context
   * @return Current OETProcessor instance
   **/
  public OETLProcessor parse(final Collection<ODocument> iBeginBlocks, final ODocument iSource, final ODocument iExtractor,
      final Collection<ODocument> iTransformers, final ODocument iLoader, final Collection<ODocument> iEndBlocks,
      final OCommandContext iContext) {
    if (iExtractor == null)
      throw new IllegalArgumentException("No Extractor configured");

    context = iContext != null ? iContext : createDefaultContext();
    init();

    try {
      String name;

      // BEGIN BLOCKS
      beginBlocks = new ArrayList<OBlock>();
      if (iBeginBlocks != null) {
        for (ODocument block : iBeginBlocks) {
          name = block.fieldNames()[0];
          final OBlock b = factory.getBlock(name);
          beginBlocks.add(b);
          configureComponent(b, (ODocument) block.field(name), iContext);
          b.execute();
        }
      }

      if (iSource != null) {
        // SOURCE
        name = iSource.fieldNames()[0];
        source = factory.getSource(name);
        configureComponent(source, (ODocument) iSource.field(name), iContext);
      } else {
        source = factory.getSource("input");
      }

      // EXTRACTOR
      name = iExtractor.fieldNames()[0];
      extractor = factory.getExtractor(name);
      configureComponent(extractor, (ODocument) iExtractor.field(name), iContext);

      if (iLoader != null) {
        // LOADER
        name = iLoader.fieldNames()[0];
        loader = factory.getLoader(name);
        configureComponent(loader, (ODocument) iLoader.field(name), iContext);
      } else {
        loader = factory.getLoader("output");
      }

      // TRANSFORMERS
      transformers = new ArrayList<OTransformer>();
      if (iTransformers != null) {
        for (ODocument t : iTransformers) {
          name = t.fieldNames()[0];
          final OTransformer tr = factory.getTransformer(name);
          transformers.add(tr);
          configureComponent(tr, (ODocument) t.field(name), iContext);
        }
      }

      // END BLOCKS
      endBlocks = new ArrayList<OBlock>();
      if (iEndBlocks != null) {
        for (ODocument block : iEndBlocks) {
          name = block.fieldNames()[0];
          final OBlock b = factory.getBlock(name);
          endBlocks.add(b);
          configureComponent(b, (ODocument) block.field(name), iContext);
        }
      }

      // analyzeFlow();

    } catch (Exception e) {
      throw new OConfigurationException("Error on creating ETL processor", e);
    }
    return this;
  }

  public OETLComponentFactory getFactory() {
    return factory;
  }

  public OETLProcessor execute() {
    if (parallel)
      executeParallel();
    else
      executeSequentially();
    return this;
  }

  public void out(final LOG_LEVELS iLogLevel, final String iText, final Object... iArgs) {
    if (logLevel.ordinal() >= iLogLevel.ordinal())
      System.out.println(String.format(iText, iArgs));
  }

  public OETLProcessorStats getStats() {
    return stats;
  }

  public OExtractor getExtractor() {
    return extractor;
  }

  public OLoader getLoader() {
    return loader;
  }

  public List<OTransformer> getTransformers() {
    return transformers;
  }

  public LOG_LEVELS getLogLevel() {
    return logLevel;
  }

  public OCommandContext getContext() {
    return context;
  }

  public boolean isParallel() {
    return threads != null;
  }

  protected void executeParallel() {
    try {
      begin();

      out(LOG_LEVELS.INFO, "Started parallel execution with %d threads", threads.length);

      if (source != null) {
        final Reader reader = source.read();

        if (reader != null)
          extractor.extract(reader);
      }

      final LinkedBlockingQueue<OExtractedItem> queue = new LinkedBlockingQueue<OExtractedItem>(threads.length * 500) {
        @Override
        public boolean offer(OExtractedItem e) {
          // turn offer() and add() into a blocking calls (unless interrupted)
          try {
            put(e);
            return true;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          return false;
        }
      };

      final AtomicLong counter = new AtomicLong();
      final AtomicBoolean extractionFinished = new AtomicBoolean(false);
      final OETLProcessor processor = this;

      for (int i = 0; i < threads.length; ++i) {

        threads[i] = new Thread(new Runnable() {
          final OETLPipeline pipeline;

          {

            pipeline = new OETLPipeline(processor, transformers, loader, logLevel, maxRetries, haltOnError);
          }

          @Override
          public void run() {
            pipeline.begin();

            while (!extractionFinished.get() || counter.get() > 0) {
              try {
                final OExtractedItem content = queue.take();
                try {
                  pipeline.execute(content);
                } finally {
                  counter.decrementAndGet();
                }
              } catch (InterruptedException e) {
              }
            }
          }
        }, "OrientDB ETL pipeline-" + i);

        threads[i].setDaemon(true);
        threads[i].start();
      }

      while (extractor.hasNext()) {
        // EXTRACTOR
        final OExtractedItem current = extractor.next();

        // TRANSFORM + LOAD
        queue.offer(current);
        counter.incrementAndGet();
      }

      extractionFinished.set(true);

      while (counter.get() > 0) {
        out(LOG_LEVELS.INFO, "Waiting for the pipeline to finish, remaining " + counter.get() + " entries to process");
        try {
          // WAIT A BIT AND RETRY
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }

      end();

    } catch (OETLProcessHaltedException e) {
      out(LOG_LEVELS.ERROR, "ETL process halted: %s", e);
    }
  }

  protected void begin() {
    out(LOG_LEVELS.INFO, "BEGIN ETL PROCESSOR");

    final Integer cfgMaxRetries = (Integer) context.getVariable("maxRetries");
    if (cfgMaxRetries != null)
      maxRetries = cfgMaxRetries;

    final Integer dumpEveryMs = (Integer) context.getVariable("dumpEveryMs");
    if (dumpEveryMs != null && dumpEveryMs > 0) {
      dumpTask = new TimerTask() {
        @Override
        public void run() {
          dumpProgress();
        }
      };

      Orient.instance().scheduleTask(dumpTask, dumpEveryMs, dumpEveryMs);

      startTime = System.currentTimeMillis();
    }

    for (OBlock t : beginBlocks) {
      t.begin();
      t.execute();
      t.end();
    }

    if (source != null)
      source.begin();
    extractor.begin();
  }

  protected void end() {
    for (OTransformer t : transformers)
      t.end();

    if (source != null)
      source.end();
    extractor.end();
    loader.end();

    for (OBlock t : endBlocks) {
      t.begin();
      t.execute();
      t.end();
    }

    elapsed = System.currentTimeMillis() - startTime;
    if (dumpTask != null) {
      dumpTask.cancel();
    }

    out(LOG_LEVELS.INFO, "END ETL PROCESSOR");

    dumpProgress();
  }

  protected void executeSequentially() {
    try {
      begin();

      if (source != null) {
        final Reader reader = source.read();

        if (reader != null)
          extractor.extract(reader);
      }

      final OETLPipeline pipeline = new OETLPipeline(this, transformers, loader, logLevel, maxRetries, haltOnError);
      pipeline.begin();

      while (extractor.hasNext()) {
        // EXTRACTOR
        final OExtractedItem current = extractor.next();

        // TRANSFORM + LOAD
        pipeline.execute(current);
      }

      end();

    } catch (OETLProcessHaltedException e) {
      out(LOG_LEVELS.ERROR, "ETL process halted: %s", e);
      throw e;
    }
  }

  protected void configureComponent(final OETLComponent iComponent, final ODocument iCfg, final OCommandContext iContext) {
    iComponent.configure(this, iCfg, iContext);
  }

  protected Class getClassByName(final OETLComponent iComponent, final String iClassName) {
    final Class inClass;
    if (iClassName.equals("ODocument"))
      inClass = ODocument.class;
    else if (iClassName.equals("String"))
      inClass = String.class;
    else if (iClassName.equals("Object"))
      inClass = Object.class;
    else if (iClassName.equals("OrientVertex"))
      inClass = OrientVertex.class;
    else if (iClassName.equals("OrientEdge"))
      inClass = OrientEdge.class;
    else
      try {
        inClass = Class.forName(iClassName);
      } catch (ClassNotFoundException e) {
        throw new OConfigurationException(
            "Class '" + iClassName + "' declared as 'input' of ETL Component '" + iComponent.getName() + "' was not found.");
      }
    return inClass;
  }

  protected void dumpProgress() {
    final long now = System.currentTimeMillis();

    final long extractorProgress = extractor.getProgress();
    final long extractorTotal = extractor.getTotal();
    final long extractorItemsSec = (long) ((extractorProgress - stats.lastExtractorProgress) * 1000f / (now - stats.lastLap));
    final String extractorUnit = extractor.getUnit();

    final long loaderProgress = loader.getProgress();
    final long loaderItemsSec = (long) ((loaderProgress - stats.lastLoaderProgress) * 1000f / (now - stats.lastLap));
    final String loaderUnit = loader.getUnit();

    final String extractorTotalFormatted = extractorTotal > -1 ? String.format("%,d", extractorTotal) : "?";

    if (extractorTotal == -1) {
      out(LOG_LEVELS.INFO,
          "+ extracted %,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
          extractorProgress, extractorUnit, extractorItemsSec, extractorUnit, extractor.getProgress(), extractor.getUnit(),
          loaderProgress, loaderUnit, loaderItemsSec, loaderUnit, OIOUtils.getTimeAsString(now - startTime), stats.warnings.get(),
          stats.errors.get());
    } else {
      float extractorPercentage = ((float) extractorProgress * 100 / extractorTotal);

      out(LOG_LEVELS.INFO,
          "+ %3.2f%% -> extracted %,d/%,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
          extractorPercentage, extractorProgress, extractorTotal, extractorUnit, extractorItemsSec, extractorUnit,
          extractor.getProgress(), extractor.getUnit(), loaderProgress, loaderUnit, loaderItemsSec, loaderUnit,
          OIOUtils.getTimeAsString(now - startTime), stats.warnings.get(), stats.errors.get());
    }

    stats.lastExtractorProgress = extractorProgress;
    stats.lastLoaderProgress = loaderProgress;
    stats.lastLap = now;
  }

  protected void analyzeFlow() {
    if (extractor == null)
      throw new OConfigurationException("extractor is null");

    if (loader == null)
      throw new OConfigurationException("loader is null");

    OETLComponent lastComponent = extractor;

    for (OTransformer t : transformers) {
      checkTypeCompatibility(t, lastComponent);
      lastComponent = t;
    }

    checkTypeCompatibility(loader, lastComponent);
  }

  protected void checkTypeCompatibility(final OETLComponent iCurrentComponent, final OETLComponent iLastComponent) {
    final String out;
    final List<String> ins;

    try {
      out = iLastComponent.getConfiguration().field("output");
      if (out == null)
        // SKIP IT
        return;

      ins = iCurrentComponent.getConfiguration().field("input");
      if (ins == null)
        // SKIP IT
        return;

      final Class outClass = getClassByName(iLastComponent, out);

      for (String in : ins) {
        final Class inClass = getClassByName(iCurrentComponent, in);
        if (inClass.isAssignableFrom(outClass)) {
          return;
        }
      }

    } catch (Exception e) {

      throw new OConfigurationException("Error on checking compatibility between components '" + iLastComponent.getName()
          + "' and '" + iCurrentComponent.getName() + "'", e);
    }

    throw new OConfigurationException("Component '" + iCurrentComponent.getName() + "' expects one of the following inputs " + ins
        + " but the 'output' for component '" + iLastComponent.getName() + "' is: " + out);

  }

  protected void init() {
    final String cfgLog = (String) context.getVariable("log");
    if (cfgLog != null)
      logLevel = LOG_LEVELS.valueOf(cfgLog.toUpperCase());

    final Boolean cfgHaltOnError = (Boolean) context.getVariable("haltOnError");
    if (cfgHaltOnError != null)
      haltOnError = cfgHaltOnError;

    final Object parallelSetting = context.getVariable("parallel");
    if (parallelSetting != null)
      parallel = (Boolean) parallelSetting;

    if (parallel) {
      int cores = Runtime.getRuntime().availableProcessors();
      if (cores > 2)
        cores = cores - 1;
      threads = new Thread[cores];
      for (int i = 0; i < cores; ++i) {
        threads[i] = new Thread("OrientDB ETL Pipeline-" + i);
      }
    }
  }

  public enum LOG_LEVELS {
    NONE, ERROR, INFO, DEBUG
  }

  public class OETLProcessorStats {
    public long       lastExtractorProgress = 0;
    public long       lastLoaderProgress    = 0;
    public long       lastLap               = 0;
    public AtomicLong warnings              = new AtomicLong();
    public AtomicLong errors                = new AtomicLong();

    public long incrementWarnings() {
      return warnings.incrementAndGet();
    }

    public long incrementErrors() {
      return errors.incrementAndGet();
    }
  }
}
