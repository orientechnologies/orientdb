package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ONodeStateStore {

  private final Optional<String> networkId;
  private final OTopologyState state;
  private final Set<ONodeId> members;
  private final long version;

  public ONodeStateStore(
      Optional<String> networkId, OTopologyState state, Set<ONodeId> members, long version) {
    super();
    this.networkId = networkId;
    this.state = state;
    this.members = members;
    this.version = version;
  }

  public Optional<String> getNetworkId() {
    return networkId;
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

  public static ONodeStateStore fromResult(OResult d) {
    assert (int) d.getProperty("serializationVersion") == 1;
    OTopologyState state = OTopologyState.valueOf(d.getProperty("state"));
    Optional<String> networkId = Optional.ofNullable(d.getProperty("networkId"));
    Set<ONodeId> members =
        ((Collection<OResult>) d.getProperty("members"))
            .stream().map((e) -> ONodeId.readResult(e)).collect(Collectors.toSet());
    long version = d.getProperty("version");
    return new ONodeStateStore(networkId, state, members, version);
  }

  public void toElement(OElement el) {
    el.setProperty("serializationVersion", 1);
    el.setProperty("state", state.name());
    el.setProperty("networkId", networkId.orElse(null));
    el.setProperty(
        "members", this.members.stream().map((x) -> x.toDocument()).collect(Collectors.toSet()));
    el.setProperty("version", version);
  }
}
