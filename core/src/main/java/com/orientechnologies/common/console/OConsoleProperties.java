package com.orientechnologies.common.console;

public class OConsoleProperties {
  public static final String VERBOSE = "verbose";
  public static final String WIDTH = "width";
  public static final String ECHO = "echo";
  public static final String IGNORE_ERRORS = "ignoreErrors";
  public static final String LIMIT = "limit";
  public static final String BACKUP_BUFFER_SIZE = "backupBufferSize";
  public static final String BACKUP_COMPRESSION_LEVEL = "backupCompressionLevel";
  public static final String DEBUG = "debug";
  public static final String COLLECTION_MAX_ITEMS = "collectionMaxItems";
  public static final String MAX_BINARY_DISPLAY = "maxBinaryDisplay";
  public static final String PROMPT_DATE_FORMAT = "promptDateFormat";
  public static final String MAX_MULTI_VALUE_ENTRIES = "maxMultiValueEntries";

  /**
   * Integer
   *
   * <p>0 for compatibility with v 3.1 or previous, 1 for compatibility with v 3.2
   */
  public static final String COMPATIBILITY_LEVEL = "compatibilityLevel";

  public static final int COMPATIBILITY_LEVEL_0 = 0;
  public static final int COMPATIBILITY_LEVEL_1 = 1;
  public static final int COMPATIBILITY_LEVEL_LATEST = COMPATIBILITY_LEVEL_1;
}
