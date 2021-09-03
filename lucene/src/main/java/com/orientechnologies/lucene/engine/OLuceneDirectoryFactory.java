package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

/** Created by frank on 03/03/2016. */
public class OLuceneDirectoryFactory {

  public static final String OLUCENE_BASE_DIR = "luceneIndexes";

  public static final String DIRECTORY_TYPE = "directory_type";

  public static final String DIRECTORY_NIO = "nio";
  public static final String DIRECTORY_MMAP = "mmap";
  public static final String DIRECTORY_RAM = "ram";

  public static final String DIRECTORY_PATH = "directory_path";

  public OLuceneDirectory createDirectory(
      final OStorage storage, final String indexName, final ODocument metadata) {
    final String luceneType =
        metadata.containsField(DIRECTORY_TYPE)
            ? metadata.<String>field(DIRECTORY_TYPE)
            : DIRECTORY_MMAP;
    if (storage.getType().equals(ODatabaseType.MEMORY.name().toLowerCase())
        || DIRECTORY_RAM.equals(luceneType)) {
      final Directory dir = new RAMDirectory();
      return new OLuceneDirectory(dir, null);
    }
    return createDirectory(storage, indexName, metadata, luceneType);
  }

  private OLuceneDirectory createDirectory(
      final OStorage storage,
      final String indexName,
      final ODocument metadata,
      final String luceneType) {
    final String luceneBasePath;
    if (metadata.containsField(DIRECTORY_PATH)) {
      luceneBasePath = metadata.<String>field(DIRECTORY_PATH);
    } else {
      luceneBasePath = OLUCENE_BASE_DIR;
    }
    final Path luceneIndexPath =
        Paths.get(storage.getConfiguration().getDirectory(), luceneBasePath, indexName);
    try {
      Directory dir = null;
      if (DIRECTORY_NIO.equals(luceneType)) {
        dir = new NIOFSDirectory(luceneIndexPath);
      } else if (DIRECTORY_MMAP.equals(luceneType)) {
        dir = new MMapDirectory(luceneIndexPath);
      }
      return new OLuceneDirectory(dir, luceneIndexPath.toString());
    } catch (final IOException e) {
      OLogManager.instance()
          .error(this, "unable to create Lucene Directory with type " + luceneType, e);
    }
    OLogManager.instance().warn(this, "unable to create Lucene Directory, FALL BACK to ramDir");
    return new OLuceneDirectory(new RAMDirectory(), null);
  }
}
