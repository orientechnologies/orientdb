package com.orientechnologies.orient.server.hazelcast.sharding.hazelcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.ODHTNode;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OHazelcastDHTNodeProxy implements ODHTNode {
  private final long              nodeId;
  private final Member            member;
  private final HazelcastInstance hazelcastInstance;

  public OHazelcastDHTNodeProxy(long nodeId, Member member, HazelcastInstance hazelcastInstance) {
    this.nodeId = nodeId;
    this.member = member;
    this.hazelcastInstance = hazelcastInstance;
  }

  public long getNodeId() {
    return nodeId;
  }

  public long getSuccessor() {
    return callOnRemoteMember(new GetSuccessorNodeCall(nodeId, member.getUuid()), false);
  }

  public Long getPredecessor() {
    return callOnRemoteMember(new GetPredecessorNodeCall(nodeId, member.getUuid()), false);
  }

  public void notify(long node) {
    callOnRemoteMember(new NotifyNodeCall(nodeId, member.getUuid(), node), true);
  }

  public boolean join(long node) {
    return callOnRemoteMember(new JoinNodeCall(nodeId, member.getUuid(), node), false);
  }

  public long findSuccessor(long id) {
    return callOnRemoteMember(new FindSuccessorNodeCall(nodeId, member.getUuid(), id), false);
  }

  public void notifyMigrationEnd(long notifierId) {
    callOnRemoteMember(new NotifyMigrationEndNodeCall(this.nodeId, member.getUuid(), notifierId), true);
  }

  public void requestMigration(long requesterId) {
    callOnRemoteMember(new RequestMigrationNodeCall(nodeId, member.getUuid(), requesterId), true);
  }

  @Override
  public OPhysicalPosition createRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iRecordVersion,
      byte iRecordType) {
    return callOnRemoteMember(new CreateRecordNodeCall(nodeId, member.getUuid(), storageName, iRecordId, iContent, iRecordVersion,
        iRecordType), false);
  }

  @Override
  public ORawBuffer readRecord(String storageName, ORID iRid) {
    return callOnRemoteMember(new LoadRecordNodeCall(nodeId, member.getUuid(), storageName, iRid), false);
  }

  @Override
  public int updateRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType) {
    return callOnRemoteMember(new UpdateRecordNodeCall(nodeId, member.getUuid(), storageName, iRecordId, iContent, iVersion,
        iRecordType), false);
  }

  @Override
  public boolean deleteRecord(String storageName, ORecordId iRecordId, int iVersion) {
    return callOnRemoteMember(new DeleteRecordNodeCall(nodeId, member.getUuid(), storageName, iRecordId, iVersion), false);
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
    try {
      Future<T> future = (Future<T>) hazelcastInstance.getExecutorService().submit(new DistributedTask<T>(call, member));

      if (async)
        return null;

      return future.get();
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error during distribution task", e);
    } catch (ExecutionException e) {
      OLogManager.instance().error(this, "Error during distribution task", e);
    }

    return null;
  }

  private static abstract class NodeCall<T> implements Callable<T>, Externalizable {
    protected long   nodeId;
    protected String memberUUID;

    public NodeCall() {
    }

    public NodeCall(long nodeId, String memberUUID) {
      this.nodeId = nodeId;
      this.memberUUID = memberUUID;
    }

    public T call() throws Exception {
      final ODHTNode node = ServerInstance.INSTANCES.get(memberUUID).findById(nodeId);
      return call(node);
    }

    protected abstract T call(ODHTNode node);

    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeLong(nodeId);
      out.writeUTF(memberUUID);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      nodeId = in.readLong();
      memberUUID = in.readUTF();
    }
  }

  private static final class GetSuccessorNodeCall extends NodeCall<Long> {
    public GetSuccessorNodeCall() {
    }

    private GetSuccessorNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.getSuccessor();
    }
  }

  private static final class GetPredecessorNodeCall extends NodeCall<Long> {
    public GetPredecessorNodeCall() {
    }

    private GetPredecessorNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.getPredecessor();
    }
  }

  private static final class JoinNodeCall extends NodeCall<Boolean> {
    private long joinNodeId;

    public JoinNodeCall() {
    }

    private JoinNodeCall(long nodeId, String memberUUID, long joinNodeId) {
      super(nodeId, memberUUID);
      this.joinNodeId = joinNodeId;
    }

    @Override
    protected Boolean call(ODHTNode node) {
      return node.join(joinNodeId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(joinNodeId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      joinNodeId = in.readLong();
    }
  }

  private static final class NotifyNodeCall extends NodeCall<Void> {
    private long notifyNodeId;

    public NotifyNodeCall() {
    }

    private NotifyNodeCall(long nodeId, String memberUUID, long notifyNodeId) {
      super(nodeId, memberUUID);
      this.notifyNodeId = notifyNodeId;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.notify(notifyNodeId);

      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(notifyNodeId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      notifyNodeId = in.readLong();
    }
  }

  private static final class FindSuccessorNodeCall extends NodeCall<Long> {
    private long keyId;

    public FindSuccessorNodeCall() {
    }

    private FindSuccessorNodeCall(long nodeId, String memberUUID, long keyId) {
      super(nodeId, memberUUID);
      this.keyId = keyId;
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.findSuccessor(keyId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(keyId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      keyId = in.readLong();
    }
  }

  private static final class NotifyMigrationEndNodeCall extends NodeCall<Void> {
    private long notifierId;

    public NotifyMigrationEndNodeCall() {
    }

    private NotifyMigrationEndNodeCall(long nodeId, String memberUUID, long notifierId) {
      super(nodeId, memberUUID);
      this.notifierId = notifierId;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.notifyMigrationEnd(notifierId);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(notifierId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      notifierId = in.readLong();
    }
  }

  private static final class RequestMigrationNodeCall extends NodeCall<Void> {
    private long requesterId;

    public RequestMigrationNodeCall() {
    }

    private RequestMigrationNodeCall(long nodeId, String memberUUID, long requesterId) {
      super(nodeId, memberUUID);
      this.requesterId = requesterId;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.requestMigration(requesterId);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(requesterId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      requesterId = in.readLong();
    }
  }

  private static final class CreateRecordNodeCall extends NodeCall<OPhysicalPosition> {

    private String    storageName;
    private ORecordId recordId;
    private byte[]    content;
    private int       recordVersion;
    private byte      recordType;

    public CreateRecordNodeCall() {
    }

    public CreateRecordNodeCall(long nodeId, String memberUUID, String storageName, ORecordId iRecordId, byte[] iContent,
        int iRecordVersion, byte iRecordType) {
      super(nodeId, memberUUID);
      this.storageName = storageName;
      this.recordId = iRecordId;
      this.content = iContent;
      this.recordVersion = iRecordVersion;
      this.recordType = iRecordType;
    }

    @Override
    protected OPhysicalPosition call(ODHTNode node) {
      return node.createRecord(storageName, recordId, content, recordVersion, recordType);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(storageName);
      out.writeObject(recordId);
      out.writeObject(content);
      out.writeInt(recordVersion);
      out.writeByte(recordType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = (String) in.readObject();
      recordId = (ORecordId) in.readObject();
      content = (byte[]) in.readObject();
      recordVersion = in.readInt();
      recordType = in.readByte();
    }
  }

  private static final class LoadRecordNodeCall extends NodeCall<ORawBuffer> {

    private String storageName;
    private ORID   iRid;

    public LoadRecordNodeCall(long nodeId, String uuid, String storageName, ORID iRid) {
      super(nodeId, uuid);
      this.storageName = storageName;
      this.iRid = iRid;
    }

    public LoadRecordNodeCall() {
    }

    @Override
    protected ORawBuffer call(ODHTNode node) {
      return node.readRecord(storageName, iRid);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(storageName);
      out.writeObject(iRid.getIdentity());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = (String) in.readObject();
      iRid = (ORID) in.readObject();
    }
  }

  private static class UpdateRecordNodeCall extends NodeCall<Integer> {
    private String    storageName;
    private ORecordId iRecordId;
    private byte[]    iContent;
    private int       iVersion;
    private byte      iRecordType;

    public UpdateRecordNodeCall() {
    }

    public UpdateRecordNodeCall(long nodeId, String uuid, String storageName, ORecordId iRecordId, byte[] iContent, int iVersion,
        byte iRecordType) {
      super(nodeId, uuid);
      this.storageName = storageName;
      this.iRecordId = iRecordId;
      this.iContent = iContent;
      this.iVersion = iVersion;
      this.iRecordType = iRecordType;
    }

    @Override
    protected Integer call(ODHTNode node) {
      return node.updateRecord(storageName, iRecordId, iContent, iVersion, iRecordType);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(storageName);
      out.writeObject(iRecordId);
      out.writeObject(iContent);
      out.writeInt(iVersion);
      out.writeByte(iRecordType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = (String) in.readObject();
      iRecordId = (ORecordId) in.readObject();
      iContent = (byte[]) in.readObject();
      iVersion = in.readInt();
      iRecordType = in.readByte();
    }
  }

  private static class DeleteRecordNodeCall extends NodeCall<Boolean> {
    private String    storageName;
    private ORecordId iRecordId;
    private int       iVersion;

    public DeleteRecordNodeCall() {
    }

    public DeleteRecordNodeCall(long nodeId, String uuid, String storageName, ORecordId iRecordId, int iVersion) {
      super(nodeId, uuid);
      this.storageName = storageName;
      this.iRecordId = iRecordId;
      this.iVersion = iVersion;
    }

    @Override
    protected Boolean call(ODHTNode node) {
      return node.deleteRecord(storageName, iRecordId, iVersion);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(storageName);
      out.writeObject(iRecordId);
      out.writeInt(iVersion);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = (String) in.readObject();
      iRecordId = (ORecordId) in.readObject();
      iVersion = in.readInt();
    }
  }
}
