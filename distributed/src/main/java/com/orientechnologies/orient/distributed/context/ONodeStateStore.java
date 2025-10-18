package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ONodeStateStore {

  private final Optional<OGroupId> groupId;
  private final OTopologyState state;
  private final Set<ONodeId> members;
  private final long version;
  private final int quorum;

  public ONodeStateStore(
      Optional<OGroupId> groupId,
      OTopologyState state,
      Set<ONodeId> members,
      int quorum,
      long version) {
    super();
    this.groupId = groupId;
    this.state = state;
    this.members = members;
    this.quorum = quorum;
    this.version = version;
  }

  public Optional<OGroupId> getGroupId() {
    return groupId;
  }

  public OTopologyState getState() {
    return state;
  }

  public Set<ONodeId> getMembers() {
    return members;
  }

  public long getVersion() {
    return version;
  }

  public int getQuorum() {
    return quorum;
  }

  public static ONodeStateStore fromResult(OResult d) {
    assert (int) d.getProperty("serializationVersion") == 1;
    OTopologyState state = OTopologyState.valueOf(d.getProperty("state"));
    Optional<OGroupId> networkId = Optional.of(OGroupId.readResult(d.getProperty("groupId")));
    Set<ONodeId> members =
        ((Collection<OResult>) d.getProperty("members"))
            .stream().map((e) -> ONodeId.readResult(e)).collect(Collectors.toSet());
    long version = d.getProperty("version");
    int quorum = d.getProperty("quorum");
    return new ONodeStateStore(networkId, state, members, quorum, version);
  }

  public void toElement(OElement el) {
    el.setProperty("serializationVersion", 1);
    el.setProperty("state", state.name());
    el.setProperty("groupId", groupId.orElse(null));
    el.setProperty("quorum", quorum);
    el.setProperty(
        "members", this.members.stream().map((x) -> x.toDocument()).collect(Collectors.toSet()));
    el.setProperty("version", version);
  }
}
