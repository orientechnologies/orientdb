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

  public static final byte CREATE_CLUSTER_CO                   = 15;
  public static final byte DELETE_CLUSTER_CO                   = 16;
  public static final byte CLUSTER_CREATE_RECORD_CO            = 17;
  public static final byte CLUSTER_DELETE_RECORD_CO            = 18;
  public static final byte CLUSTER_ALLOCATE_RECORD_POSITION_CO = 19;
  public static final byte CLUSTER_UPDATE_RECORD_CO            = 20;

  public static final byte INDEX_ENGINE_CREATE_CO = 21;
  public static final byte INDEX_ENGINE_DELETE_CO = 22;

  public static final byte CELL_BTREE_SINGLE_VALUE_PUT_CO    = 23;
  public static final byte CELL_BTREE_SINGLE_VALUE_REMOVE_CO = 24;

  public static final byte CELL_BTREE_MULTI_VALUE_PUT_CO          = 25;
  public static final byte CELL_BTREE_MULTI_VALUE_REMOVE_ENTRY_CO = 26;

  public static final byte SBTREE_PUT_CO    = 27;
  public static final byte SBTREE_REMOVE_CO = 28;

  public static final byte LOCAL_HASHTABLE_PUT_CO    = 29;
  public static final byte LOCAL_HASHTABLE_REMOVE_CO = 30;

  public static final byte SBTREE_BONSAI_CREATE_COMPONENT_CO = 31;
  public static final byte SBTREE_BONSAI_CREATE_CO           = 32;
  public static final byte SBTREE_BONSAI_DELETE_COMPONENT_CO = 33;
  public static final byte SBTREE_BONSAI_DELETE_CO           = 34;

  public static final byte CLUSTER_POSITION_MAP_INIT_PO                = 35;
  public static final byte CLUSTER_POSITION_MAP_ADD_PO                 = 36;
  public static final byte CLUSTER_POSITION_MAP_ALLOCATE_PO            = 37;
  public static final byte CLUSTER_POSITION_MAP_TRUNCATE_LAST_ENTRY_PO = 38;
  public static final byte CLUSTER_POSITION_MAP_UPDATE_ENTRY_PO        = 40;
  public static final byte CLUSTER_POSITION_MAP_UPDATE_STATUS_PO       = 41;

  public static final byte CLUSTER_PAGE_INIT_PO                  = 42;
  public static final byte CLUSTER_PAGE_APPEND_RECORD_PO         = 43;
  public static final byte CLUSTER_PAGE_REPLACE_RECORD_PO        = 44;
  public static final byte CLUSTER_PAGE_DELETE_RECORD_PO         = 45;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE_PO         = 46;
  public static final byte CLUSTER_PAGE_SET_PREV_PAGE_PO         = 47;
  public static final byte CLUSTER_PAGE_SET_RECORD_LONG_VALUE_PO = 48;

  public static final byte PAGINATED_CLUSTER_STATE_V0_SET_SIZE_PO           = 49;
  public static final byte PAGINATED_CLUSTER_STATE_V0_SET_RECORDS_SIZE_PO   = 50;
  public static final byte PAGINATED_CLUSTER_STATE_V0_SET_FREE_LIST_PAGE_PO = 51;
}
