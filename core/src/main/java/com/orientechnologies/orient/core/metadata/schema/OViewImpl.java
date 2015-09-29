package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.*;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public class OViewImpl extends OSchemaObject implements OView {
  private String                    name;
  private String                    query;

  protected OViewImpl(final OSchemaShared iOwner, final ODocument iDocument) {
    super(iOwner, iDocument);
  }

  protected OViewImpl(final OSchemaShared owner, String name, String query) {
    super(owner);
    this.name = name;
    this.query = query;
  }

  @Override
  public String getName() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public static void verifyName(String iName) {
    if (iName == null || iName.isEmpty()) {
      throw new OSchemaException("Invalid empty view name!");
    }

    final Character wrongCharacter = OSchemaShared.checkViewNameIfValid(iName);
    if (wrongCharacter != null) {
      throw new OSchemaException("Invalid view name found. Character '" + wrongCharacter + "' cannot be used in view name '"
        + iName + "'");
    }
  }

  @Override
  public OView setName(String iName) {
    OViewImpl.verifyName(iName);

    String selfName = getName();
    if (selfName.equals(iName)) {
      return this;
    }

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    {
      acquireSchemaWriteLock();
      try {
        OSchema schema = getDatabase().getMetadata().getSchema();

        String key = iName.toLowerCase();
        OView view = schema.getView(key);
        if (view != null) {
          throw new OSchemaException("Cannot rename view " + selfName + " to " + iName +
            ". A view with such name already exists");
        }
        if (schema.getClass(key) != null) {
          throw new OSchemaException("Cannot rename view " + selfName + " to " + iName +
            ". A class with such name already exists");
        }

        final ODatabaseDocumentInternal database = getDatabase();
        final OStorage storage = database.getStorage();

        if (storage instanceof OStorageProxy) {
          database.command(new OCommandSQL("ALTER VIEW " + selfName + " NAME " + name)).execute();
        } else if (isDistributedCommand()) {
          final OCommandSQL commandSQL = new OCommandSQL("ALTER VIEW " + selfName + " NAME " + name);
          commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
          database.command(commandSQL).execute();

          setNameInternal(iName);
        } else {
          setNameInternal(iName);
        }
      } finally {
        releaseSchemaWriteLock();
      }
    }

    return this;
  }

  private void setNameInternal(final String iName) {
    checkEmbedded();

    owner.changeViewName(this.name, iName, this);
    this.name = iName;
  }

  private void setQueryInternal(final String iQuery) {
    checkEmbedded();
    this.query = iQuery;
  }

  @Override
  public String getQuery() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return query;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OView setQuery(String iQuery) {
    if (iQuery != null) {
      iQuery = iQuery.trim();
    }
    if (iQuery == null || iQuery.isEmpty()) {
      throw new OSchemaException("Invalid empty query input at view " + this.name);
    }

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL("ALTER VIEW " + this.name + " QUERY " + iQuery)).execute();
      } else if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL("ALTER VIEW " + this.name + " QUERY " + iQuery);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
        setQueryInternal(iQuery);
      } else
        setQueryInternal(iQuery);
    } finally {
      releaseSchemaWriteLock();
    }
    return null;
  }

  @Override
  protected void loadDocument() {
    /* name */
    this.name = document.field("name");

    /* query */
    if (document.containsField("query")) {
      this.query = document.field("query");
    }
  }

  @Override
  protected void saveDocument() {
    document.field("name", this.name);
    document.field("query", this.query);
  }

  @Override
  protected int calculateHashCode(int baseHashCode) {
    return  17 * baseHashCode + (name != null ? name.hashCode() : 0);
  }
}
