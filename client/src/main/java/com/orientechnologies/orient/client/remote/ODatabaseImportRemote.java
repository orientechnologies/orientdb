package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImpExpAbstract;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseTool;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/** Created by tglman on 19/07/16. */
public class ODatabaseImportRemote extends ODatabaseImpExpAbstract {

  private String options;

  public ODatabaseImportRemote(
      ODatabaseDocumentInternal iDatabase, String iFileName, OCommandOutputListener iListener) {
    super(iDatabase, iFileName, iListener);
  }

  @Override
  public void run() {
    try {
      importDatabase();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during database import", e);
    }
  }

  @Override
  public ODatabaseTool setOptions(String iOptions) {
    this.options = iOptions;
    return super.setOptions(iOptions);
  }

  public void importDatabase() throws ODatabaseImportException {
    File file = new File(getFileName());
    try {
      ODatabaseDocument db = getDatabase();
      if (db instanceof ODatabaseDocumentTx) {
        db = ((ODatabaseDocumentTx) db).getUnderlying();
      }
      ((ODatabaseDocumentRemote) db)
          .importDatabase(options, new FileInputStream(file), file.getName(), getListener());
    } catch (FileNotFoundException e) {
      throw OException.wrapException(
          new ODatabaseImportException("Error importing the database"), e);
    }
  }

  public void close() {}
}
