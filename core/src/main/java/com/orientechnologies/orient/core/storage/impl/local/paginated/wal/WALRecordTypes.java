package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

@SuppressWarnings("WeakerAccess")
public final class WALRecordTypes {
  public static final byte UPDATE_PAGE_RECORD                    = 0;
  public static final byte FUZZY_CHECKPOINT_START_RECORD         = 1;
  public static final byte FUZZY_CHECKPOINT_END_RECORD           = 2;
  public static final byte FULL_CHECKPOINT_START_RECORD          = 4;
  public static final byte CHECKPOINT_END_RECORD                 = 5;
  public static final byte ATOMIC_UNIT_START_RECORD              = 8;
  public static final byte ATOMIC_UNIT_END_RECORD                = 9;
  public static final byte FILE_CREATED_WAL_RECORD               = 10;
  public static final byte NON_TX_OPERATION_PERFORMED_WAL_RECORD = 11;
  public static final byte FILE_DELETED_WAL_RECORD               = 12;
  public static final byte FILE_TRUNCATED_WAL_RECORD             = 13;
  public static final byte EMPTY_WAL_RECORD                      = 14;
  public static final byte ALLOCATE_POSITION_OPERATION           = 15;
  public static final byte CREATE_RECORD_OPERATION               = 16;
  public static final byte DELETE_RECORD_OPERATION               = 17;
  public static final byte RECYCLE_RECORD_OPERATION              = 18;
  public static final byte UPDATE_RECORD_OPERATION               = 19;
  public static final byte HASH_TABLE_PUT_OPERATION              = 20;
  public static final byte HASH_TABLE_REMOVE_OPERATION           = 21;
  public static final byte SBTREE_PUT_OPERATION                  = 22;
  public static final byte SBTREE_REMOVE_OPERATION               = 23;
  public static final byte SBTREE_BONSAI_PUT_OPERATION           = 24;
  public static final byte SBTREE_BONSAI_REMOVE_OPERATION        = 25;
  public static final byte CREATE_CLUSTER_OPERATION              = 26;
  public static final byte CREATE_HASH_TABLE_OPERATION           = 27;
  public static final byte CREATE_SBTREE_OPERATION               = 28;
  public static final byte CREATE_SBTREE_BONSAI_OPERATION        = 29;
  public static final byte CREATE_SBTREE_BONSAI_RAW_OPERATION    = 30;
  public static final byte MAKE_POSITION_AVAILABLE_OPERATION     = 31;
}
