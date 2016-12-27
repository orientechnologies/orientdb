package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by frank on 03/03/2016.
 */
public class OLuceneDirectoryFactory {

  public static final String OLUCENE_BASE_DIR = "luceneIndexes";

  public static final String DIRECTORY_TYPE = "directory_type";

  public static final String DIRECTORY_NIO  = "nio";
  public static final String DIRECTORY_MMAP = "mmap";
  public static final String DIRECTORY_RAM  = "ram";

  public static final String DIRECTORY_PATH = "directory_path";

  public Directory createDirectory(ODatabaseDocumentInternal database, String indexName, ODocument metadata) {

    String luceneType = metadata.containsField(DIRECTORY_TYPE) ? metadata.<String>field(DIRECTORY_TYPE) : DIRECTORY_MMAP;

    if (database.getStorage().getType().equals("memory") || DIRECTORY_RAM.equals(luceneType)) {
      return new RAMDirectory();
    }

    return createDirectory(database, indexName, metadata, luceneType);
  }

  private Directory createDirectory(ODatabaseDocumentInternal database, String indexName, ODocument metadata, String luceneType) {
    String luceneBasePath = metadata.containsField(DIRECTORY_PATH) ? metadata.<String>field(DIRECTORY_PATH) : OLUCENE_BASE_DIR;

    Path luceneIndexPath = Paths.get(database.getStorage().getConfiguration().getDirectory(), luceneBasePath, indexName);
    try {

      if (DIRECTORY_NIO.equals(luceneType)) {
        return new NIOFSDirectory(luceneIndexPath);
      }

      if (DIRECTORY_MMAP.equals(luceneType)) {
        return new MMapDirectory(luceneIndexPath);
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "unable to create Lucene Directory with type " + luceneType, e);
    }

    OLogManager.instance().warn(this, "unable to create Lucene Directory, FALL BACK to ramDir");
    return new RAMDirectory();
  }

}
