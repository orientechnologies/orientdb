package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

public class ONewDeltaSyncImporter {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ONewDeltaSyncImporter.class);

  public static boolean importDelta(
      OrientDBDistributed context, String databaseName, InputStream in, String targetNode) {
    final String nodeName = context.getDistributedManager().getLocalNodeName();
    try {

      OScenarioThreadLocal.executeAsDistributed(
          new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              logger.infoIn(
                  nodeName, targetNode, "Started import of delta for database '%s'", databaseName);
              final ODatabaseDocumentInternal db = context.openNoAuthorization(databaseName);
              DataInput dataInput = new DataInputStream(in);
              while (dataInput.readBoolean()) {
                OTransactionData transaction = OTransactionData.read(dataInput);
                db.syncCommit(transaction);
              }

              return null;
            }
          });
      return true;
    } catch (OException e) {
      logger.error("Error running delta sync import", e);
      return false;
    }
  }
}
