package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import java.nio.file.Path;

public class OStorageEnginePaginatedLocalEngerprise extends OStorageEnginePaginatedLocal {

  protected OLocalPaginatedStorage newLocalInstance(
      OrientDBInternal context, String name, Path path) {
    return new OEnterpriseLocalPaginatedStorage(
        name,
        path.toString(),
        generateStorageId(),
        readCache,
        files,
        maxWALSegmentSize,
        doubleWriteLogMaxSegSize,
        context);
  }
}
