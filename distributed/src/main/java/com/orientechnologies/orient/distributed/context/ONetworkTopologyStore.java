package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public record ONetworkTopologyStore(
    OGroupId groupId, OTopologyState state, Set<ONodeId> members, int quorum, OVersion version) {

  public static ONetworkTopologyStore fromResult(OResult d) {
    assert (int) d.getProperty("serializationVersion") == 1;
    OTopologyState state = OTopologyState.valueOf(d.getProperty("state"));
    OGroupId networkId = OGroupId.readResult(d.getProperty("groupId"));
    Set<ONodeId> members =
        ((Collection<OResult>) d.getProperty("members"))
            .stream().map((e) -> ONodeId.readResult(e)).collect(Collectors.toSet());
    var version = OVersion.fromResult(d);
    int quorum = d.getProperty("quorum");
    return new ONetworkTopologyStore(networkId, state, members, quorum, version);
  }

  public void toElement(OElement el) {
    el.setProperty("serializationVersion", 1);
    el.setProperty("state", state.name());
    el.setProperty("groupId", groupId.toDocument());
    el.setProperty("quorum", quorum);
    el.setProperty(
        "members",
        this.members.stream().map((x) -> x.toDocument()).collect(Collectors.toList()),
        OType.EMBEDDEDLIST);
    this.version.toElement(el);
  }
}
