package com.orientechnologies.enterprise.server.listener;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;

/** Created by Enrico Risa on 16/07/2018. */
public interface OEnterpriseStorageListener {

  default void onCreate(OEnterpriseLocalPaginatedStorage storage) {}

  default void onOpen(OEnterpriseLocalPaginatedStorage database) {}

  default void onClose(OEnterpriseLocalPaginatedStorage database) {}

  default void onDrop(OEnterpriseLocalPaginatedStorage database) {}

  default void onCommandStart(ODatabase database, OResultSet result) {}

  default void onCommandEnd(ODatabase database, OResultSet result) {}
}
