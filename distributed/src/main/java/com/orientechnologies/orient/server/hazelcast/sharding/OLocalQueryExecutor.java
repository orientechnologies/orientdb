package com.orientechnologies.orient.server.hazelcast.sharding;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

/**
 * Executor for undistributable commands such as insert/update/delete (distribution will be performed by storage CRUD operations)
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:10
 */
public class OLocalQueryExecutor extends OQueryExecutor {

  public OLocalQueryExecutor(OCommandRequestText iCommand, OStorageEmbedded wrapped) {
    super(iCommand, wrapped);
  }

  @Override
  public Object execute() {
    return wrapped.command(iCommand);
  }
}
