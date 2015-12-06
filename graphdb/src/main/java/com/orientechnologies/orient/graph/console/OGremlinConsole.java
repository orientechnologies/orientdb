/*
 * Copyright 2010-2014 Orient Technologies LTD (info--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.graph.console;

import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.command.OCommandExecutorNotFoundException;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.graph.graphml.OGraphMLReader;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.migration.OGraphMigration;
import com.tinkerpop.blueprints.impls.orient.OBonsaiTreeRepair;
import com.tinkerpop.blueprints.impls.orient.OGraphRepair;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Gremlin specialized console.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OGremlinConsole extends OConsoleDatabaseApp {

  public OGremlinConsole(final String[] args) {
    super(args);
  }

  public static void main(final String[] args) {
    int result;
    try {
      boolean tty = false;
      try {
        if (setTerminalToCBreak())
          tty = true;

      } catch (Exception e) {
      }

      final OConsoleDatabaseApp console = new OGremlinConsole(args);
      if (tty)
        console.setReader(new TTYConsoleReader());

      result = console.run();

    } finally {
      try {
        stty("echo");
      } catch (Exception e) {
      }
    }
    System.exit(result);
  }

  @ConsoleCommand(splitInWords = false, description = "Execute a GREMLIN script")
  public void gremlin(
      @ConsoleParameter(name = "script-text", description = "The script text to execute") final String iScriptText) {
    checkForDatabase();

    if (iScriptText == null || iScriptText.length() == 0)
      return;

    resetResultSet();

    long start = System.currentTimeMillis();
    try {
      final Object result = currentDatabase.command(new OCommandGremlin(iScriptText)).execute();

      float elapsedSeconds = (System.currentTimeMillis() - start) / 1000;

      out.println("\n" + result);

      out.printf("\nScript executed in %f sec(s).", elapsedSeconds);
    } catch (OStorageException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof OCommandExecutorNotFoundException)
        out.printf("\nError: the GREMLIN command executor is not installed, check your configuration");
    }
  }

  @Override
  @ConsoleCommand(description = "Import a database into the current one", splitInWords = false)
  public void importDatabase(@ConsoleParameter(name = "options", description = "Import options") String text) throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');
    final String fileName = items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);
    final String options = fileName != null ? text.substring((items.get(0)).length() + (items.get(1)).length() + 1).trim() : text;

    if (fileName != null && (fileName.endsWith(".graphml") || fileName.endsWith(".xml"))) {
      message("\nImporting GRAPHML database from " + fileName + " with options (" + options + ")...");

      try {
        final Map<String, List<String>> opts = parseOptions(options);

        final OrientGraph g = new OrientGraph(currentDatabase);
        g.setUseLog(false);
        g.setWarnOnForceClosingTx(false);
        new OGraphMLReader(g).setOptions(opts).inputGraph(g, fileName);
        g.commit();
        currentDatabase.commit();

      } catch (ODatabaseImportException e) {
        printError(e);
      }
    } else
      super.importDatabase(text);
  }

  @Override
  @ConsoleCommand(description = "Repair database structure")
  public void repairDatabase(@ConsoleParameter(name = "options", description = "Options: -v", optional = true) String iOptions)
      throws IOException {
    checkForDatabase();

    final boolean fix_graph = iOptions == null || iOptions.contains("--fix-graph");
    if (fix_graph) {
      // REPAIR GRAPH
      new OGraphRepair().repair(new OrientGraphNoTx(currentDatabase), this);
    }

    final boolean fix_links = iOptions == null || iOptions.contains("--fix-links");
    if (fix_links) {
      // REPAIR DATABASE AT LOW LEVEL
      super.repairDatabase(iOptions);
    }

    if (!currentDatabase.getURL().startsWith("plocal")) {
      message("\n fix-bonsai can be run only on plocal connection \n");
      return;
    }

    final boolean fix_ridbags = iOptions == null || iOptions.contains("--fix-ridbags");
    if (fix_ridbags) {
      OBonsaiTreeRepair repairer = new OBonsaiTreeRepair();
      repairer.repairDatabaseRidbags(currentDatabase, this);
    }

    final boolean fix_bonsai = iOptions == null || iOptions.contains("--fix-bonsai");
    if (fix_bonsai) {
      OBonsaiTreeRepair repairer = new OBonsaiTreeRepair();
      repairer.repairDatabaseRidbags(currentDatabase, this);
    }
  }

  @ConsoleCommand(description = "Migrates graph from OMVRBTree to ORidBag")
  public void upgradeGraph() {
    OGraphMigration migration = new OGraphMigration(getCurrentDatabase(), this);
    migration.execute();
    message("Graph has been upgraded.");
  }

  @Override
  protected void onBefore() {
    super.onBefore();

    out.println("\nInstalling extensions for GREMLIN language v." + OGremlinHelper.getEngineVersion());

    OGremlinHelper.global().create();
  }

  @Override
  protected boolean isCollectingCommands(final String iLine) {
    return super.isCollectingCommands(iLine) || iLine.startsWith("gremlin");
  }
}
