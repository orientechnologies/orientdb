package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public final class WALRecordTypes {
  public static final int UPDATE_PAGE_RECORD = 0;
  public static final int ATOMIC_UNIT_START_RECORD = 8;
  public static final int ATOMIC_UNIT_END_RECORD = 9;
  public static final int FILE_CREATED_WAL_RECORD = 10;
  public static final int NON_TX_OPERATION_PERFORMED_WAL_RECORD = 11;
  public static final int FILE_DELETED_WAL_RECORD = 12;
  public static final int FILE_TRUNCATED_WAL_RECORD = 13;
  public static final int EMPTY_WAL_RECORD = 14;
  public static final int ATOMIC_UNIT_START_METADATA_RECORD = 15;
  public static final int HIGH_LEVEL_TRANSACTION_CHANGE_RECORD = 18;
  public static final int TX_METADATA = 194;
  public static final int FREE_SPACE_MAP_INIT = 195;
  public static final int FREE_SPACE_MAP_UPDATE = 196;
}
