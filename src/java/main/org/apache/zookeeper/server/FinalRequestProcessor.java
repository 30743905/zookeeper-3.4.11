/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.TxnHeader;

import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.OpResult.ErrorResult;

/**
 * This Request processor actually applies any transaction associated with a
 * request and services any queries. It is always at the end of a
 * RequestProcessor chain (hence the name), so it does not have a nextProcessor
 * member.
 *
 * This RequestProcessor counts on ZooKeeperServer to populate the
 * outstandingRequests member of ZooKeeperServer.
 *
 *
 * FinalRequestProcessor负责把已经commit的写操作应用到本机，对于读操作则从本机中读取数据并返回给client，这个processor是责任链中的最后一个
 * FinalRequestProcessor是一个同步处理的processor，主要的处理逻辑就在方法processRequest中：
 *
 *      1、如果request.hdr != null，则表明request是写操作，则调用zks.processTxn(hdr, txn)来把request关联的写操作执行到内存数据库中
 *      2、如果是写操作，则调用zks.getZKDatabase().addCommittedProposal(request);把request加入到ZKDatabase.committedLog队列中，
 *         这个队列主要是为了快速和follower同步而保留的，详见zookeeper leader和learner的数据同步
 *
 *      3、为各类操作准备响应数据，对于写操作则根据processTxn的结果来回复，如果是读操作，则读取内存中的状态，发送响应数据给client
 *
 *
 *
 * processRequest里面，事务和非事务的处理方式：
        1、事务的话，统一调用 zks.processTxn(hdr, txn)， 底层dataTree统一处理
        2、非事务的话，在当前类写了一堆代码，比如getData请求
 *
 */
public class FinalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FinalRequestProcessor.class);

    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    public void processRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (request.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', request, "");
        }
        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            while (!zks.outstandingChanges.isEmpty()
                    && zks.outstandingChanges.get(0).zxid <= request.zxid) {//outstandingChanges不为空且首个元素的zxid小于请求的zxid
                // 移除首个元素
                ChangeRecord cr = zks.outstandingChanges.remove(0);
                if (cr.zxid < request.zxid) {//只会出现<= (当前请求是事务请求是，按照顺序应该是=，如果不是的话，应该是<)
                    LOG.warn("Zxid outstanding "
                            + cr.zxid
                            + " is less than current " + request.zxid);
                }
                if (zks.outstandingChangesForPath.get(cr.path) == cr) {// 根据路径得到Record并判断是否为cr
                    // 移除cr的路径对应的记录
                    zks.outstandingChangesForPath.remove(cr.path);
                }
            }
            if (request.hdr != null) {//如果是事务
                // 获取请求头
               TxnHeader hdr = request.hdr;
                // 获取事务体
               Record txn = request.txn;
                // 处理事务
               rc = zks.processTxn(hdr, txn);//把事务请求应用到内存数据库
            }
            // do not add non quorum packets to the queue.
            if (Request.isQuorum(request.type)) {// 只将quorum包（事务性请求）添加进队列，判断是否是事务请求
                /**
                 * 事务日志记了什么:
                        1、只要产生了事务请求，即使还没有提交，都会记录到事务日志中去，即使提交，并且最终处理了，也不会有什么日志变化
                 代码中 zks.getZKDatabase().addCommittedProposal(request); 并没有修改，生成日志文件

                 zks.getZKDatabase().addCommittedProposal(request)：事务请求，则把request加入到ZKDatabase.committedLog队列中

                 事务请求最终提交，处理完生效，为什么不需要日志了
                    前面没提交的时候生成事务日志，这里完成成功了，倒不需要记录了？
                 */
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }

        if (request.hdr != null && request.hdr.getType() == OpCode.closeSession) { // 请求头不为空并且请求类型为关闭会话
            ServerCnxnFactory scxn = zks.getServerCnxnFactory();
            // this might be possible since
            // we might just be playing diffs from the leader
            if (scxn != null && request.cnxn == null) {
                // calling this if we have the cnxn results in the client's
                // close session response being lost - we've already closed
                // the session/socket here before we can send the closeSession
                // in the switch block below
                // 关闭会话
                scxn.closeSession(request.sessionId);
                return;
            }
        }

        if (request.cnxn == null) { // 请求的cnxn为空，直接返回
            return;
        }
        ServerCnxn cnxn = request.cnxn;

        String lastOp = "NA";
        zks.decInProcess();
        Code err = Code.OK;
        Record rsp = null;
        boolean closeSession = false;
        try {
            if (request.hdr != null && request.hdr.getType() == OpCode.error) {
                throw KeeperException.create(KeeperException.Code.get((
                        (ErrorTxn) request.txn).getErr()));
            }

            KeeperException ke = request.getException();
            if (ke != null && request.type != OpCode.multi) {
                throw ke;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}",request);
            }
            switch (request.type) {
            case OpCode.ping: {
                // 更新延迟
                zks.serverStats().updateLatency(request.createTime);

                lastOp = "PING";
                // 更新响应的状态
                cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                        request.createTime, Time.currentElapsedTime());
                // 设置响应
                cnxn.sendResponse(new ReplyHeader(-2,
                        zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                return;
            }
            case OpCode.createSession: {// 创建会话请求
                // 更新延迟
                zks.serverStats().updateLatency(request.createTime);

                lastOp = "SESS";
                // 更新响应的状态
                cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                        request.createTime, Time.currentElapsedTime());
                // 结束会话初始化
                zks.finishSessionInit(request.cnxn, true);
                return;
            }
            case OpCode.multi: {// 多重操作
                lastOp = "MULT";
                rsp = new MultiResponse() ;

                for (ProcessTxnResult subTxnResult : rc.multiResult) { // 遍历多重操作结果

                    OpResult subResult ;

                    switch (subTxnResult.type) {// 确定每个操作类型
                        case OpCode.check://检查
                            subResult = new CheckResult();
                            break;
                        case OpCode.create:// 创建
                            subResult = new CreateResult(subTxnResult.path);
                            break;
                        case OpCode.delete:// 删除
                            subResult = new DeleteResult();
                            break;
                        case OpCode.setData:// 设置数据
                            subResult = new SetDataResult(subTxnResult.stat);
                            break;
                        case OpCode.error:// 错误
                            subResult = new ErrorResult(subTxnResult.err) ;
                            break;
                        default:
                            throw new IOException("Invalid type of op");
                    }
                    // 添加至响应结果集中
                    ((MultiResponse)rsp).add(subResult);
                }

                break;
            }
            case OpCode.create: {
                lastOp = "CREA";
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.delete: {
                lastOp = "DELE";
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setData: {
                lastOp = "SETD";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setACL: {
                lastOp = "SETA";
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.closeSession: {
                lastOp = "CLOS";
                closeSession = true;
                err = Code.get(rc.err);
                break;
            }
            case OpCode.sync: {
                lastOp = "SYNC";
                SyncRequest syncRequest = new SyncRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        syncRequest);
                rsp = new SyncResponse(syncRequest.getPath());
                break;
            }
            case OpCode.check: {
                lastOp = "CHEC";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.exists: {
                lastOp = "EXIS";
                // TODO we need to figure out the security requirement for this!
                ExistsRequest existsRequest = new ExistsRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        existsRequest);
                String path = existsRequest.getPath();
                if (path.indexOf('\0') != -1) {
                    throw new KeeperException.BadArgumentsException();
                }
                Stat stat = zks.getZKDatabase().statNode(path, existsRequest
                        .getWatch() ? cnxn : null);
                rsp = new ExistsResponse(stat);
                break;
            }
            case OpCode.getData: {
                lastOp = "GETD";
                GetDataRequest getDataRequest = new GetDataRequest();
                //反序列化出getDataRequest
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getDataRequest);
                DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                if (n == null) {//验证path对应的node是否存在
                    throw new KeeperException.NoNodeException();
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);//验证ACL权限
                Stat stat = new Stat();
                byte b[] = zks.getZKDatabase().getData(getDataRequest.getPath(), stat,
                        getDataRequest.getWatch() ? cnxn : null);//如果有watch标志位，Watcher就传cnxn
                rsp = new GetDataResponse(b, stat);
                break;
            }
            case OpCode.setWatches: {
                lastOp = "SETW";
                SetWatches setWatches = new SetWatches();
                // XXX We really should NOT need this!!!!
                request.request.rewind();
                ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                long relativeZxid = setWatches.getRelativeZxid();
                zks.getZKDatabase().setWatches(relativeZxid, 
                        setWatches.getDataWatches(), 
                        setWatches.getExistWatches(),
                        setWatches.getChildWatches(), cnxn);
                break;
            }
            case OpCode.getACL: {
                lastOp = "GETA";
                GetACLRequest getACLRequest = new GetACLRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getACLRequest);
                Stat stat = new Stat();
                List<ACL> acl = 
                    zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                rsp = new GetACLResponse(acl, stat);
                break;
            }
            case OpCode.getChildren: {
                lastOp = "GETC";
                GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getChildrenRequest);
                DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildrenRequest.getPath(), null, getChildrenRequest
                                .getWatch() ? cnxn : null);
                rsp = new GetChildrenResponse(children);
                break;
            }
            case OpCode.getChildren2: {
                lastOp = "GETC";
                GetChildren2Request getChildren2Request = new GetChildren2Request();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getChildren2Request);
                Stat stat = new Stat();
                DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildren2Request.getPath(), stat, getChildren2Request
                                .getWatch() ? cnxn : null);
                rsp = new GetChildren2Response(children, stat);
                break;
            }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then 
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        long lastZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();
        ReplyHeader hdr =
            new ReplyHeader(request.cxid, lastZxid, err.intValue());

        zks.serverStats().updateLatency(request.createTime);
        cnxn.updateStatsForResponse(request.cxid, lastZxid, lastOp,
                    request.createTime, Time.currentElapsedTime());

        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG",e);
        }
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}
