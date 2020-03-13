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

  public static final byte CLUSTER_PAGE_ADD_RECORD_OPERATION            = 32;
  public static final byte CLUSTER_PAGE_DELETER_RECORD_OPERATION        = 33;
  public static final byte CLUSTER_PAGE_REPLACE_RECORD_OPERATION        = 34;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE_OPERATION         = 35;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE_POINTER_OPERATION = 36;
  public static final byte CLUSTER_PAGE_SET_PREV_PAGE_OPERATION         = 37;

  public static final byte CLUSTER_POSITION_MAP_BUCKET_ADD_OPERATION            = 38;
  public static final byte CLUSTER_POSITION_MAP_BUCKET_ALLOCATE_OPERATION       = 39;
  public static final byte CLUSTER_POSITION_MAP_BUCKET_SET_OPERATION            = 40;
  public static final byte CLUSTER_POSITION_MAP_BUCKET_RESURRECT_OPERATION      = 41;
  public static final byte CLUSTER_POSITION_MAP_BUCKET_MAKE_AVAILABLE_OPERATION = 42;
  public static final byte CLUSTER_POSITION_MAP_BUCKET_REMOVE_OPERATION         = 43;

  public static final byte PAGINATED_CLUSTER_STATE_SET_SIZE_OPERATION           = 44;
  public static final byte PAGINATED_CLUSTER_STATE_SET_RECORD_SIZE_OPERATION    = 45;
  public static final byte PAGINATED_CLUSTER_STATE_SET_FREE_LIST_PAGE_OPERATION = 46;

  public static final byte SBTREE_BUCKET_SET_SIZE_OPERATION                        = 47;
  public static final byte SBTREE_BUCKET_SET_VALUE_FREE_LIST_FIRST_INDEX_OPERATION = 48;
  public static final byte SBTREE_BUCKET_REMOVE_OPERATION                          = 49;
  public static final byte SBTREE_BUCKET_ADD_ALL_OPERATION                         = 50;
  public static final byte SBTREE_BUCKET_SHRINK_OPERATION                          = 51;
  public static final byte SBTREE_BUCKET_ADD_ENTRY_OPERATION                       = 52;
  public static final byte SBTREE_BUCKET_UPDATE_VALUE_OPERATION                    = 53;
  public static final byte SBTREE_BUCKET_SET_LEFT_SIBLING_OPERATION                = 54;
  public static final byte SBTREE_BUCKET_SET_RIGHT_SIBLING_OPERATION               = 55;

  public static final byte SBTREE_BONSAI_BUCKET_SET_SIZE_OPERATION              = 56;
  public static final byte SBTREE_BONSAI_BUCKET_REMOVE_OPERATION                = 57;
  public static final byte SBTREE_BONSAI_BUCKET_ADD_ALL_OPERATION               = 58;
  public static final byte SBTREE_BONSAI_BUCKET_SHRINK_OPERATION                = 59;
  public static final byte SBTREE_BONSAI_BUCKET_ADD_ENTRY_OPERATION             = 60;
  public static final byte SBTREE_BONSAI_BUCKET_UPDATE_VALUE_OPERATION          = 61;
  public static final byte SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER_OPERATION = 62;
  public static final byte SBTREE_BONSAI_BUCKET_SET_DELETED_OPERATION           = 63;
  public static final byte SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING_OPERATION      = 64;
  public static final byte SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING_OPERATION     = 65;

  public static final byte FILE_DELETED_WAL_RECORD_V2   = 66;
  public static final byte FILE_TRUNCATED_WAL_RECORD_V2 = 67;
  public static final byte ATOMIC_UNIT_END_RECORD_V2    = 68;
  public static final byte FILE_CREATED_WAL_RECORD_V2   = 69;
  public static final byte UPDATE_PAGE_RECORD_V2        = 70;
  public static final byte ATOMIC_UNIT_START_RECORD_V2  = 71;

  public static final int ATOMIC_UNIT_START_METADATA_RECORD      = 72;
  public static final int FULL_CHECKPOINT_START_METADATA_RECORD  = 73;
  public static final int FUZZY_CHECKPOINT_START_METADATA_RECORD = 74;
  public static final int HIGH_LEVEL_TRANSACTION_CHANGE_RECORD   = 75;

}
