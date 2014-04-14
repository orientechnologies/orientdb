package com.orientechnologies.lucene.utils;

import java.io.File;

/**
 * Created by enricorisa on 08/04/14.
 */
public class OLuceneIndexUtils {

  public static void deleteFolder(File folder) {
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteFolder(f);
        } else {
          f.delete();
        }
      }
    }
    folder.delete();
  }
}
