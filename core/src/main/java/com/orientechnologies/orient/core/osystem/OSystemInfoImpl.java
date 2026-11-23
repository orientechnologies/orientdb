package com.orientechnologies.orient.core.osystem;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import java.util.UUID;

public class OSystemInfoImpl implements OSystemInfo {

  public static final String SERVER_INFO_CLASS = "ServerInfo";
  public static final String SERVER_ID_PROPERTY = "serverId";
  public static final String NODE_NAME_PROPERTY = "nodeName";

  private final OSystemDatabase system;

  public OSystemInfoImpl(OSystemDatabase system) {
    this.system = system;
  }

  private interface ToUpdate {
    void change(OElement doc);
  }

  private synchronized void update(ToUpdate toUpdate) {

    try (ODatabaseDocumentInternal db = system.openSystemDatabase()) {
      OClass clazz = db.getClass(SERVER_INFO_CLASS);
      if (clazz == null) {
        db.createClassIfNotExist(SERVER_INFO_CLASS);
        clazz = db.getClass(SERVER_INFO_CLASS);
      }
      OElement info;
      if (clazz.count() == 0) {
        info = db.newElement(SERVER_INFO_CLASS);
      } else {
        info = db.browseClass(clazz.getName()).next();
      }
      toUpdate.change(info);
      db.save(info);
    }
  }

  private interface ToRead<T> {
    T read(OElement doc);
  }

  private synchronized <T> T read(ToRead<T> toRead) {
    try (ODatabaseDocumentInternal db = system.openSystemDatabase()) {
      OClass clazz = db.getClass(SERVER_INFO_CLASS);
      if (clazz == null) {
        return null;
      }
      OElement info = db.browseClass(clazz.getName()).next();
      if (info == null) {
        return null;
      }
      return toRead.read(info);
    }
  }

  public synchronized void checkServerId() {
    update(
        (info) -> {
          String serverId = info.getProperty(SERVER_ID_PROPERTY);
          if (serverId == null) {
            serverId = UUID.randomUUID().toString();
            info.setProperty(SERVER_ID_PROPERTY, serverId);
          }
        });
  }

  @Override
  public ONodeId getNodeId() {
    checkServerId();
    return read((info) -> new ONodeId(info.getProperty(SERVER_ID_PROPERTY)));
  }

  @Override
  public String getNodeName() {
    return read((info) -> info.getProperty(NODE_NAME_PROPERTY));
  }

  @Override
  public void setNodeName(String nodeName) {
    update((info) -> info.setProperty(NODE_NAME_PROPERTY, nodeName));
  }
}
