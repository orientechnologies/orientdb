package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

@SuppressWarnings("WeakerAccess")
public final class WALRecordTypes {
  public static final int UPDATE_PAGE_RECORD                    = 0;
  public static final int FUZZY_CHECKPOINT_START_RECORD         = 1;
  public static final int FUZZY_CHECKPOINT_END_RECORD           = 2;
  public static final int FULL_CHECKPOINT_START_RECORD          = 4;
  public static final int CHECKPOINT_END_RECORD                 = 5;
  public static final int ATOMIC_UNIT_START_RECORD              = 8;
  public static final int ATOMIC_UNIT_END_RECORD                = 9;
  public static final int FILE_CREATED_WAL_RECORD               = 10;
  public static final int NON_TX_OPERATION_PERFORMED_WAL_RECORD = 11;
  public static final int FILE_DELETED_WAL_RECORD               = 12;
  public static final int FILE_TRUNCATED_WAL_RECORD             = 13;
  public static final int EMPTY_WAL_RECORD                      = 14;

  public static final int CREATE_CLUSTER_CO                   = 15;
  public static final int DELETE_CLUSTER_CO                   = 16;
  public static final int CLUSTER_CREATE_RECORD_CO            = 17;
  public static final int CLUSTER_DELETE_RECORD_CO            = 18;
  public static final int CLUSTER_ALLOCATE_RECORD_POSITION_CO = 19;
  public static final int CLUSTER_UPDATE_RECORD_CO            = 20;

  public static final int INDEX_ENGINE_CREATE_CO = 21;
  public static final int INDEX_ENGINE_DELETE_CO = 22;

  public static final int CELL_BTREE_SINGLE_VALUE_PUT_CO    = 23;
  public static final int CELL_BTREE_SINGLE_VALUE_REMOVE_CO = 24;

  public static final int CELL_BTREE_MULTI_VALUE_PUT_CO          = 25;
  public static final int CELL_BTREE_MULTI_VALUE_REMOVE_ENTRY_CO = 26;

  public static final int SBTREE_PUT_CO    = 27;
  public static final int SBTREE_REMOVE_CO = 28;

  public static final int LOCAL_HASHTABLE_PUT_CO    = 29;
  public static final int LOCAL_HASHTABLE_REMOVE_CO = 30;

  public static final int SBTREE_BONSAI_CREATE_COMPONENT_CO = 31;
  public static final int SBTREE_BONSAI_CREATE_CO           = 32;
  public static final int SBTREE_BONSAI_DELETE_COMPONENT_CO = 33;
  public static final int SBTREE_BONSAI_DELETE_CO           = 34;

  public static final int CLUSTER_POSITION_MAP_INIT_PO                = 35;
  public static final int CLUSTER_POSITION_MAP_ADD_PO                 = 36;
  public static final int CLUSTER_POSITION_MAP_ALLOCATE_PO            = 37;
  public static final int CLUSTER_POSITION_MAP_TRUNCATE_LAST_ENTRY_PO = 38;
  public static final int CLUSTER_POSITION_MAP_UPDATE_ENTRY_PO        = 40;
  public static final int CLUSTER_POSITION_MAP_UPDATE_STATUS_PO       = 41;

  public static final int CLUSTER_PAGE_INIT_PO                  = 42;
  public static final int CLUSTER_PAGE_APPEND_RECORD_PO         = 43;
  public static final int CLUSTER_PAGE_REPLACE_RECORD_PO        = 44;
  public static final int CLUSTER_PAGE_DELETE_RECORD_PO         = 45;
  public static final int CLUSTER_PAGE_SET_NEXT_PAGE_PO         = 46;
  public static final int CLUSTER_PAGE_SET_PREV_PAGE_PO         = 47;
  public static final int CLUSTER_PAGE_SET_RECORD_LONG_VALUE_PO = 48;

  public static final int PAGINATED_CLUSTER_STATE_V0_SET_SIZE_PO           = 49;
  public static final int PAGINATED_CLUSTER_STATE_V0_SET_RECORDS_SIZE_PO   = 50;
  public static final int PAGINATED_CLUSTER_STATE_V0_SET_FREE_LIST_PAGE_PO = 51;

  public static final int PAGINATED_CLUSTER_STATE_V1_SET_FREE_LIST_PAGE_PO = 52;
  public static final int PAGINATED_CLUSTER_STATE_V1_SET_RECORDS_SIZE_PO   = 53;
  public static final int PAGINATED_CLUSTER_STATE_V1_SET_SIZE_PO           = 54;
  public static final int PAGINATED_CLUSTER_STATE_V1_SET_FILE_SIZE_PO      = 55;

  public static final int PAGINATED_CLUSTER_STATE_V2_SET_FILE_SIZE_PO      = 56;
  public static final int PAGINATED_CLUSTER_STATE_V2_SET_FREE_LIST_PAGE_PO = 57;
  public static final int PAGINATED_CLUSTER_STATE_V2_SET_RECORDS_SIZE_PO   = 58;
  public static final int PAGINATED_CLUSTER_STATE_V2_SET_SIZE_PO           = 59;

  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_INIT_PO                  = 60;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_LEAF_ENTRY_PO        = 61;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_NON_LEAF_ENTRY_PO    = 62;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_LEAF_ENTRY_PO     = 63;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_NON_LEAF_ENTRY_PO = 64;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_ALL_PO               = 65;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SHRINK_PO                = 66;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_UPDATE_VALUE_PO          = 67;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_LEFT_SIBLING_PO      = 68;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_RIGHT_SIBLING_PO     = 69;

  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_INIT_PO         = 70;
  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_SET_VALUE_PO    = 71;
  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_REMOVE_VALUE_PO = 72;

  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_INIT_PO           = 73;
  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_TREE_SIZE_PO  = 74;
  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_PAGES_SIZE_PO = 75;

  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SWITCH_BUCKET_TYPE_PO = 76;

  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_INIT_PO                  = 77;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_LEAF_ENTRY_PO        = 78;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_NON_LEAF_ENTRY_PO    = 79;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_LEAF_ENTRY_PO     = 80;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_ALL_PO               = 81;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SHRINK_PO                = 82;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_UPDATE_VALUE_PO          = 83;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_NON_LEAF_ENTRY_PO = 84;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_LEFT_SIBLING_PO      = 85;
  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_RIGHT_SIBLING_PO     = 86;

  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_INIT_PO           = 87;
  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_PAGES_SIZE_PO = 88;
  public static final int CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_TREE_SIZE_PO  = 89;

  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_INIT_PO         = 90;
  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_SET_VALUE_PO    = 91;
  public static final int CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_REMOVE_VALUE_PO = 92;

  public static final int CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SWITCH_BUCKET_TYPE_PO = 93;

  public static final int SBTREE_BUCKET_V1_INIT_PO                  = 94;
  public static final int SBTREE_BUCKET_V1_ADD_LEAF_ENTRY_PO        = 95;
  public static final int SBTREE_BUCKET_V1_ADD_NON_LEAF_ENTRY_PO    = 96;
  public static final int SBTREE_BUCKET_V1_REMOVE_LEAF_ENTRY_PO     = 97;
  public static final int SBTREE_BUCKET_V1_REMOVE_NON_LEAF_ENTRY_PO = 98;
  public static final int SBTREE_BUCKET_V1_ADD_ALL_PO               = 99;
  public static final int SBTREE_BUCKET_V1_SHRINK_PO                = 100;
  public static final int SBTREE_BUCKET_V1_UPDATE_VALUE_PO          = 101;
  public static final int SBTREE_BUCKET_V1_SWITCH_BUCKET_TYPE_PO    = 102;
  public static final int SBTREE_BUCKET_V1_SET_LEFT_SIBLING_PO      = 103;
  public static final int SBTREE_BUCKET_V1_SET_RIGHT_SIBLING_PO     = 104;

  public static final int SBTREE_NULL_BUCKET_V1_INIT_PO         = 105;
  public static final int SBTREE_NULL_BUCKET_V1_SET_VALUE_PO    = 106;
  public static final int SBTREE_NULL_BUCKET_V1_REMOVE_VALUE_PO = 107;

  public static final int SBTREE_BUCKET_V2_INIT_PO                  = 108;
  public static final int SBTREE_BUCKET_V2_ADD_ALL_PO               = 109;
  public static final int SBTREE_BUCKET_V2_ADD_LEAF_ENTRY_PO        = 110;
  public static final int SBTREE_BUCKET_V2_ADD_NON_LEAF_ENTRY_PO    = 111;
  public static final int SBTREE_BUCKET_V2_REMOVE_LEAF_ENTRY_PO     = 112;
  public static final int SBTREE_BUCKET_V2_REMOVE_NON_LEAF_ENTRY_PO = 113;
  public static final int SBTREE_BUCKET_V2_SET_LEFT_SIBLING_PO      = 114;
  public static final int SBTREE_BUCKET_V2_SET_RIGHT_SIBLING_PO     = 115;
  public static final int SBTREE_BUCKET_V2_SHRINK_PO                = 116;
  public static final int SBTREE_BUCKET_V2_SWITCH_BUCKET_TYPE_PO    = 117;
  public static final int SBTREE_BUCKET_V2_UPDATE_VALUE_PO          = 118;

  public static final int SBTREE_BUCKET_V1_SET_TREE_SIZE_PO = 119;
  public static final int SBTREE_BUCKET_V2_SET_TREE_SIZE_PO = 120;

  public static final int SBTREE_NULL_BUCKET_V2_INIT_PO         = 121;
  public static final int SBTREE_NULL_BUCKET_V2_REMOVE_VALUE_PO = 122;
  public static final int SBTREE_NULL_BUCKET_V2_SET_VALUE_PO    = 123;

  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_INIT_PO                    = 124;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_CREATE_MAIN_LEAF_ENTRY_PO  = 125;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_MAIN_LEAF_ENTRY_PO  = 126;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_APPEND_NEW_LEAF_ENTRY_PO   = 127;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_LEAF_ENTRY_PO       = 128;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_INCREMENT_ENTRIES_COUNT_PO = 129;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_DECREMENT_ENTRIES_COUNT_PO = 130;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_NON_LEAF_ENTRY_PO      = 131;
  public static final int CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_NON_LEAF_ENTRY_PO   = 132;
}
