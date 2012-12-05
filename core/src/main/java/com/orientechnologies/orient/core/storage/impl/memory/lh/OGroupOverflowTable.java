package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
// TODO optimize it to use Array instead of list!
class OGroupOverflowTable {

  public static class GroupOverflowInfo {
    byte group;
    int  startingPage;

    public GroupOverflowInfo(byte group, int startingPage) {
      this.group = group;
      this.startingPage = startingPage;
    }
  }

  List<GroupOverflowInfo>   overflowInfo       = new LinkedList<GroupOverflowInfo>();
  private static final byte DUMMY_GROUP_NUMBER = -1;

  public OGroupOverflowTable() {
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, 2));
  }

  public void clear() {
    overflowInfo.clear();
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, 2));
  }

  public byte getGroupWithStartingPageLessThenOrEqual(int pageToStore) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupOverflowInfo.startingPage <= pageToStore) {
        return groupOverflowInfo.group;
      }
    }
    return -2;
  }

  public int[] searchForGroupOrCreate(byte groupNumber, int groupSize) {
    int dummyGroup = -1;
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++) {
      GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);

      if (groupOverflowInfo.group == groupNumber) {
        return new int[] { groupOverflowInfo.startingPage, overflowInfo.get(i + 1).startingPage - groupOverflowInfo.startingPage };
      }

      if (dummyGroup == -1 && groupOverflowInfo.group == DUMMY_GROUP_NUMBER) {
        dummyGroup = i;
      }
    }

    if (dummyGroup == -1) {
      dummyGroup = overflowInfo.size() - 1;
    }

    // search is not successful so create new group on place of first dummy group
    // assert overflowInfo.get(dummyGroup).group == DUMMY_GROUP_NUMBER;
    overflowInfo.get(dummyGroup).group = groupNumber;

    createDummyGroupIfNeeded(groupSize);

    assert overflowInfo.get(dummyGroup).startingPage <= overflowInfo.get(overflowInfo.size() - 1).startingPage;
    return new int[] { overflowInfo.get(dummyGroup).startingPage, groupSize };
  }

  public int getPageForGroup(byte groupNumber) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupOverflowInfo.group == groupNumber) {
        return groupOverflowInfo.startingPage;
      }
    }
    return -1;
  }

  public GroupOverflowInfo findDummyGroup() {
    return findDummyGroup(0);
  }

  public int move(byte groupNumber, int groupSize) {
    removeGroup(groupNumber);
    GroupOverflowInfo dummyGroup = findDummyGroup(groupSize);
    dummyGroup.group = groupNumber;
    createDummyGroupIfNeeded(groupSize);
    return dummyGroup.startingPage;
  }

  private GroupOverflowInfo findDummyGroup(int minGroupSize) {
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++) {
      if ((overflowInfo.get(i).group == DUMMY_GROUP_NUMBER)
          && ((overflowInfo.get(i + 1).startingPage - overflowInfo.get(i).startingPage) >= minGroupSize)) {
        return overflowInfo.get(i);
      }
    }

    if (overflowInfo.get(overflowInfo.size() - 1).group == DUMMY_GROUP_NUMBER) {
      return overflowInfo.get(overflowInfo.size() - 1);
    }
    return null;

  }

  private void removeGroup(byte groupNumber) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupOverflowInfo.group == groupNumber) {
        overflowInfo.remove(groupOverflowInfo);
        break;
      }
    }
  }

  private void createDummyGroupIfNeeded(int groupSize) {
    if (findDummyGroup() == null) {
      createDummyGroup(groupSize);
    }
    if (overflowInfo.get(overflowInfo.size() - 1).group != DUMMY_GROUP_NUMBER) {
      createDummyGroup(groupSize);
    }
  }

  private void createDummyGroup(int groupSize) {
    int startingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage;
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, startingPage + groupSize));
  }

  public void moveDummyGroup(final int groupSize) {
    if (isSecondDummyGroupExists()) {
      removeGroup(DUMMY_GROUP_NUMBER);
    } else {
      overflowInfo.get(overflowInfo.size() - 1).startingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage + groupSize;
    }
  }

  public void moveDummyGroupIfNeeded(int lastPage, int groupSize) {
    if (findDummyGroup().startingPage <= lastPage) {
      moveDummyGroup(groupSize);
      collapseDummyGroups();
    }
  }

  public void removeUnusedGroups(OPageIndicator pageIndicator) {
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++) {
      for (int j = overflowInfo.get(i).startingPage; j < overflowInfo.get(i + 1).startingPage; ++j) {
        if (pageIndicator.get(j)) {
          break;
        } else if (j == overflowInfo.get(i + 1).startingPage - 1) {
          overflowInfo.get(i).group = DUMMY_GROUP_NUMBER;
        }
      }
    }
    collapseDummyGroups();
  }

  private void collapseDummyGroups() {
    for (int i = overflowInfo.size() - 1; i > 0; --i) {
      if (overflowInfo.get(i).group == DUMMY_GROUP_NUMBER && overflowInfo.get(i - 1).group == DUMMY_GROUP_NUMBER) {
        GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);
        overflowInfo.remove(groupOverflowInfo);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      builder.append("|\t").append(groupOverflowInfo.group).append("\t|\t").append(groupOverflowInfo.startingPage).append("\t|\n");
    }
    return builder.toString();
  }

  public List<GroupOverflowInfo> getOverflowGroupsInfoToMove(int page) {
    List<GroupOverflowInfo> result = new ArrayList<GroupOverflowInfo>(overflowInfo.size());
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupOverflowInfo.startingPage <= page) {
        result.add(groupOverflowInfo);
      }
    }
    return result;
  }

  public boolean isSecondDummyGroupExists() {
    int counter = 0;
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupOverflowInfo.group == DUMMY_GROUP_NUMBER) {
        counter++;
      }
    }
    return counter > 1;
  }

  public int getSizeForGroup(byte groupNumber) {
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++) {
      GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);
      if (groupOverflowInfo.group == groupNumber) {
        return overflowInfo.get(i + 1).startingPage - groupOverflowInfo.startingPage;
      }
    }
    return -1;
  }

  public int enlargeGroupSize(final byte groupNumber, final int newGroupSize) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo) {
      if (groupNumber == groupOverflowInfo.group) {
        groupOverflowInfo.group = DUMMY_GROUP_NUMBER;
        break;
      }
    }
    assert overflowInfo.get(overflowInfo.size() - 1).group == DUMMY_GROUP_NUMBER;
    int newStartingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage;
    overflowInfo.set(overflowInfo.size() - 1, new GroupOverflowInfo(groupNumber, newStartingPage));
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, newStartingPage + newGroupSize));

    return newStartingPage;
  }
}
