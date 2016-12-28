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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.block.OETLBlock;
import com.orientechnologies.orient.etl.extractor.OETLExtractor;
import com.orientechnologies.orient.etl.loader.OETLLoader;
import com.orientechnologies.orient.etl.source.OETLSource;
import com.orientechnologies.orient.etl.transformer.OETLTransformer;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ETL processor class.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli-at-orientdb.com)
 */
public class OETLProcessor {
  protected final OETLComponentFactory  factory;
  protected final OETLProcessorStats    stats;
  private final   ExecutorService       executor;
  protected       List<OETLBlock>       endBlocks;
  protected       List<OETLTransformer> transformers;
  protected       List<OETLBlock>       beginBlocks;
  protected       OETLSource            source;
  protected       OETLExtractor         extractor;
  protected       OETLLoader            loader;
  protected       OCommandContext       context;
  protected       long                  startTime;
  protected       long                  elapsed;
  protected       TimerTask             dumpTask;
  protected Level   logLevel    = Level.INFO;
  protected boolean haltOnError = true;
  protected int     maxRetries  = 10;
  protected int     workers     = 1;
  private   boolean parallel    = false;

  /**
   * Creates an ETL processor by setting all the components on construction.
   *
   * @param iBeginBlocks  List of Blocks to execute at the beginning of processing
   * @param iSource       Source component
   * @param iExtractor    Extractor component
   * @param iTransformers List of Transformers
   * @param iLoader       Loader component
   * @param iEndBlocks    List of Blocks to execute at the end of processing
   * @param iContext      Execution Context
   */
  public OETLProcessor(final List<OETLBlock> iBeginBlocks,
      final OETLSource iSource,
      final OETLExtractor iExtractor,
      final List<OETLTransformer> iTransformers,
      final OETLLoader iLoader,
      final List<OETLBlock> iEndBlocks,
      final OCommandContext iContext) {
    beginBlocks = iBeginBlocks;
    source = iSource;
    extractor = iExtractor;
    transformers = iTransformers;
    loader = iLoader;
    endBlocks = iEndBlocks;
    context = iContext;
    factory = new OETLComponentFactory();
    stats = new OETLProcessorStats();

    executor = Executors.newCachedThreadPool();

    configRunBehaviour(context);
  }

  public static void main(final String[] args) {

    System.out.println("OrientDB etl v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    if (args.length == 0) {
      System.out.println("Syntax error, missing configuration file.");
      System.out.println("Use: oetl.sh <json-file>");
      System.exit(1);
    }

    final OETLProcessor processor = new OETLProcessorConfigurator().parseConfigAndParameters(args);

    processor.execute();
  }

  protected void configRunBehaviour(OCommandContext context) {
    final String cfgLog = (String) context.getVariable("log");
    if (cfgLog != null)
      logLevel = Level.parse(cfgLog.toUpperCase());

    final Boolean cfgHaltOnError = (Boolean) context.getVariable("haltOnError");
    if (cfgHaltOnError != null)
      haltOnError = cfgHaltOnError;

    final Object parallelSetting = context.getVariable("parallel");
    if (parallelSetting != null)
      parallel = (Boolean) parallelSetting;

    if (parallel) {
      final int cores = Runtime.getRuntime().availableProcessors();

      if (cores >= 2)
        workers = cores - 1;
    }

  }

  public OETLProcessorStats getStats() {
    return stats;
  }

  public OETLExtractor getExtractor() {
    return extractor;
  }

  public OETLSource getSource() {
    return source;
  }

  public OETLLoader getLoader() {
    return loader;
  }

  public List<OETLTransformer> getTransformers() {
    return transformers;
  }

  public Level getLogLevel() {
    return logLevel;
  }

  public OCommandContext getContext() {
    return context;
  }

  protected void execute() {
    configure();
    begin();
    runExtractorAndPipeline();
    end();
  }

  private void configure() {
  }

  private void runExtractorAndPipeline() {
    try {

//      out(LOG_LEVELS.INFO, "Started execution with %d worker threads", workers);

      OLogManager.instance().info(this, "Started execution with %d worker threads", workers);
      extractor.extract(source.read());

      BlockingQueue<OETLExtractedItem> queue = new LinkedBlockingQueue<OETLExtractedItem>(workers * 500);

      final AtomicLong counter = new AtomicLong();

      List<CompletableFuture<Void>> futures = IntStream.range(0, workers).boxed()
          .map(i -> CompletableFuture.runAsync(
              new OETLPipelineWorker(queue,
                  new OETLPipeline(this, transformers, loader, logLevel, maxRetries, haltOnError)),
              executor))
          .collect(Collectors.toList());

      futures.add(CompletableFuture.runAsync(new OETLExtractorWorker(this, queue, counter), executor));

      futures.forEach(cf -> cf.join());

      OLogManager.instance().debug(this, "all items extracted");
//      out(DEBUG, "all items extracted");

      executor.shutdown();
    } catch (OETLProcessHaltedException e) {
//      out(ERROR, "ETL process halted: %s", e);
      OLogManager.instance().error(this, "ETL process halted: ", e);
      executor.shutdownNow();
    } catch (Exception e) {
//      out(ERROR, "ETL process has problem: %s", e);
      OLogManager.instance().error(this, "ETL process has problem: ", e);

//      e.printStackTrace();
      executor.shutdownNow();
    }
  }

  protected void begin() {
//    out(LOG_LEVELS.INFO, "BEGIN ETL PROCESSOR");

    OLogManager.instance().info(this, "BEGIN ETL PROCESSOR");
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

    for (OETLBlock t : beginBlocks) {
      t.begin();
      t.execute();
      t.end();
    }

    if (source != null)
      source.begin();
    extractor.begin();
  }

  protected void end() {
    for (OETLTransformer t : transformers)
      t.end();

    if (source != null)
      source.end();
    extractor.end();
    loader.end();

    for (OETLBlock t : endBlocks) {
      t.begin();
      t.execute();
      t.end();
    }

    elapsed = System.currentTimeMillis() - startTime;
    if (dumpTask != null) {
      dumpTask.cancel();
    }

    OLogManager.instance().info(this, "END ETL PROCESSOR");
    dumpProgress();
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
//      out(LOG_LEVELS.INFO,
//          "+ extracted %,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
//          extractorProgress, extractorUnit, extractorItemsSec, extractorUnit, extractor.getProgress(), extractor.getUnit(),
//          loaderProgress, loaderUnit, loaderItemsSec, loaderUnit, OIOUtils.getTimeAsString(now - startTime), stats.warnings.get(),
//          stats.errors.get());

      OLogManager.instance().info(this,
          "+ extracted %,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
          extractorProgress, extractorUnit, extractorItemsSec, extractorUnit, extractor.getProgress(), extractor.getUnit(),
          loaderProgress, loaderUnit, loaderItemsSec, loaderUnit, OIOUtils.getTimeAsString(now - startTime), stats.warnings.get(),
          stats.errors.get());

    } else {
      float extractorPercentage = ((float) extractorProgress * 100 / extractorTotal);

//      out(LOG_LEVELS.INFO,
//          "+ %3.2f%% -> extracted %,d/%,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
//          extractorPercentage, extractorProgress, extractorTotal, extractorUnit, extractorItemsSec, extractorUnit,
//          extractor.getProgress(), extractor.getUnit(), loaderProgress, loaderUnit, loaderItemsSec, loaderUnit,
//          OIOUtils.getTimeAsString(now - startTime), stats.warnings.get(), stats.errors.get());
      OLogManager.instance().info(this,
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

    for (OETLTransformer t : transformers) {
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

      throw OException.wrapException(new OConfigurationException(
          "Error on checking compatibility between components '" + iLastComponent.getName() + "' and '" + iCurrentComponent
              .getName() + "'"), e);
    }

    throw new OConfigurationException("Component '" + iCurrentComponent.getName() + "' expects one of the following inputs " + ins
        + " but the 'output' for component '" + iLastComponent.getName() + "' is: " + out);

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

  public OETLComponentFactory getFactory() {
    return factory;
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
