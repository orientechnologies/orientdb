package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.OStorageRecordOperation;
import java.util.Collection;

/** Created by Enrico Risa on 20/07/2018. */
public interface OEnterpriseStorageOperationListener {

  void onCommit(Collection<OStorageRecordOperation> operations);

  void onRollback();

  void onRead();
}
