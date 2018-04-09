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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 *
 * LearnerHandler其实是Thread的子类,这段代码的死循环就是请求处理的核心代码。如果QuorumPacket的类型是Request，那么将对报文进行简单的处理，然后提交给leader成员所指向的LeaderZooKeeperServer。而这个submitRequest()函数才是真正地实现了“责任链”的触发。而这个提交请求的函数做的事情也很简单，就是把request放到一个请求队列中。
 *
 * Leader抽象出来的与Follower节点进行信息交互的对象。同时，该对象维护了Leader与Follower之间通信状态（如是否出现网络不通情况）
 * Follower节点启动后，会主动连接Leader，而Leader会监听Follower的建立连接的请求。并为每个Follower的tcp连接创建一个LearnerHandler对象，该对象会：
    1、接收Follower发来的请求，可能包括以下请求：Follower转发的客户端的更新请求（命令类型：Leader.REQUEST），Follower对Leader的Proposal命令的回复消息ACK，Follower给Leader发送的PING（Follower会给Leader发送PING消息？）；
    2、给Follower发送心跳消息。

 为了保证整个集群内部的实时通信，同时为了确保可以控制所有的Follower/Observer服务器，Leader服务器会与每个Follower/Observer服务器建立一个TCP长连接。
 同时也会为每个Follower/Observer服务器创建一个名为LearnerHandler的实体。LearnerHandler是Learner服务器的管理者，
 主要负责Follower/Observer服务器和Leader服务器之间的一系列网络通信，包括数据同步、请求转发和Proposal提议的投票等。
 Leader服务器中保存了所有Follower/Observer对应的LearnerHandler。

 LearnerHandler主要是处理Leader和Learner之间的交互.
 Leader和每个Learner连接会维持一个长连接，并有一个单独的LearnerHandler线程和一个Learner进行交互
 可以认为，Learner和LearnerHandler是一一对应的关系.
 */
public class LearnerHandler extends ZooKeeperThread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;    

    public Socket getSocket() {
        return sock;
    }

    final Leader leader;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    //下一个接收ack的deadline，启动时(数据同步)是一个标准，完成启动后(正常交互)，是另一个标准
    volatile long tickOfNextAckDeadline;
    
    /**
     * ZooKeeper server identifier of this learner
     */
    //当前这个learner的sid
    protected long sid = 0;
    
    long getSid(){
        return sid;
    }
    //当前这个learner的version
    protected int version = 0x1;
    
    int getVersion() {
    	return version;
    }
    
    /**
     * The packets to be sent to the learner
     */
    //待发送packet的队列
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
        new LinkedBlockingQueue<QuorumPacket>();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     *
     *
     * SyncLimitCheck：作用就是控制leader等待当前learner给proposal回复ACK的时间
     在Leader发出proposal时更新对应时间，zxid记录
     在Leader收到对应ACK时，清除对应zxid的记录
     检查时，判断当前时间和最早已经发出proposal但是没有收到ack的时间对比，看是否超时


     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;//最久一次更新了但是没有收到ack的proposal的zxid
        private long currentTime = 0;//最久一次更新了但是没有收到ack的proposal的时间
        private long nextZxid = 0;//最新一次更新了但是没有收到ack的proposal的zxid
        private long nextTime = 0;//最新一次更新了但是没有收到ack的proposal的时间

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {//发送proposal时，更新提议的统计时间
            if (!started) {
                return;
            }
            if (currentTime == 0) {//如果还没初始化就初始化
                currentTime = time;
                currentZxid = zxid;
            } else {//如果已经初始化，就记录下一次的时间
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {//收到Learner关于zxid的ack了，更新ack的统计时间
             if (currentZxid == zxid) {//如果是刚刚发送的ack
                 currentTime = nextTime;//传递到下一个记录
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {//如果旧的ack还没收到 但是收到了 新的ack
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) {//如果没有等待超时，返回true
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;//当前时间与最久一次没收到ack的proposal的时间差
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };
    //proposal，ack检测
    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private final BufferedInputStream bufferedInput;
    private BufferedOutputStream bufferedOutput;

    LearnerHandler(Socket sock, BufferedInputStream bufferedInput,
                   Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        this.bufferedInput = bufferedInput;
        try {
            leader.self.authServer.authenticate(sock,
                    new DataInputStream(bufferedInput));
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection",
                    sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    //代表一个关闭shutdown的packet来关闭发送packet的线程
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType  learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     * 消费 不断消费发送队列，遇到proposalOfDeath就break，遇到Proposal则进行对应的记录
     */
    private void sendPackets() throws InterruptedException {//消费queuedPackets，发送消息，直到接受到proposalOfDeath的packet
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {//如果调用了shutDown
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {//更新当前proposal的时间统计
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type = null;
        String mess = null;
        Record txn = null;
        
        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;    
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     *
     * run方法是这个类最核心的方法，完成了和Learner的启动时的数据同步，同步完成后进行正常的交互
     *
     */
    @Override
    public void run() {
        try {
            leader.addLearnerHandler(this);
            //初始化，是leader当前周期(leader.self.tick) 再加上初始化以及同步的limit(initLimit + syncLimit)
            tickOfNextAckDeadline = leader.self.tick.get()
                    + leader.self.initLimit + leader.self.syncLimit;

            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            QuorumPacket qp = new QuorumPacket();
            /**
             * 第一步、接收FOLLOWERINFO或OBSERVERINFO数据包，该数据包主要包含Learner节点的sid、protocolVersion、type(区分Follower还是Observer)和epoch
             *    该数据包主要功能：获取到集群中过半节点的epoch，然后找出一个最大的epoch，新epoch就是在这个最大epoch的基础上加1
             */
            ia.readRecord(qp, "packet");//接收Follower发送过来的FOLLOWERINFO数据包
            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){
            	LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            //接收learner发送过来的LearnerInfo,包含sid
            byte learnerInfoData[] = qp.getData();
            if (learnerInfoData != null) {
            	if (learnerInfoData.length == 8) {
            		ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
            		this.sid = bbsid.getLong();
            	} else {
            	    //正常会执行到这里
            		LearnerInfo li = new LearnerInfo();
            		ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
            		this.sid = li.getServerid();//对端sid
            		this.version = li.getProtocolVersion();//默认版本一般是65536
            	}
            } else {
            	this.sid = leader.followerCounter.getAndDecrement();
            }

            LOG.info("Follower sid: " + sid + " : info : "
                    + leader.self.quorumPeers.get(sid));
                        
            if (qp.getType() == Leader.OBSERVERINFO) {
                  learnerType = LearnerType.OBSERVER;
            }
            //获取对端Follower节点发送过来的epoch
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
            
            long peerLastZxid;
            StateSummary ss = null;
            long zxid = qp.getZxid();
            //计算出新的epoch，计算原则：上一任epoch基础上加1，计算结束标志：过半服务器参与计算，如果未过半参与则当前线程执行到这里会进入休眠状态
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            
            if (this.getVersion() < 0x10000) {//leader是旧版本
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                leader.waitForEpochAck(this.getSid(), ss);
            } else {//leader是新版本
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                /**
                 * 第二步、计算出新epoch后，接下来就通过构建一个LEADERINFO数据包，该数据包中带有新newEpoch值，并将该数据包广播到集群中的所有Learner节点，
                 *    完成newEpoch同步工作
                 */
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                bufferedOutput.flush();

                /**
                 * 第三步、Learner服务器在收到带有newEpoch的LEADERINFO数据包后，会回复一个ACKEPOCH数据包
                 *    ACKEPOCH数据包同时会把Learner服务器的lastZxid带过来，接下来Leader和Learner数据同步采用哪种方式
                 *    就是根据这个lastZxid进行匹配判断确定
                 */
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				        }
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                leader.waitForEpochAck(this.getSid(), ss);//阻塞直到收到过半的ACKEPOCH数据包
            }
            peerLastZxid = ss.getLastZxid();
            
            //packetToSend表示Leader和Follower同步方式，默认采用SNAP方式，SNAP即采用镜像快照进行全量同步
            /**
             * 第四步、接下来就进入到Leader和Learner数据同步阶段
             *    默认采用的同步方式是SNAP，具体采用的同步方式是根据lastZxid进行匹配确定
             *
             *    同步大致的流程：
             *      1、确定同步方式后，会向Learner服务器发送同步方式数据包(SNAP+DIFF+TRUNC)，让Learner知道接下来如何进行数据同步，Learner可能会进行一些前期的准备工作
             *      2、然后发送Proposal+Commit或Snapshot镜像数据进行同步工作
             *      3、最后Leader会向Learner发送NEWLEADER数据包，代表Leader已经将所有需要同步的数据包发送完毕了，Learner收到NEWLEADER数据包会回复一个ACK数据包
             *      4、当收到过半回复的ACK数据包后，表示Leader端进行数据同步完成，Leader最后会向Learner发送UPTODATE数据包，Learner接收到UPTODATE数据包数据包后知道集群同步已经完成了，Learner会退出同步流程
             */
            int packetToSend = Leader.SNAP;
            long zxidToSend = 0;
            long leaderLastZxid = 0;
            /** the packets that the follower needs to get updates from **/
            long updates = peerLastZxid;
            
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */ 
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();
            ReadLock rl = lock.readLock();
            try {
                rl.lock();


                /**
                 * 1、committedLog是用来保存最近一段时间内执行的事务请求议案，个数限制默认为500个议案
                 * 2、ZK启动的时候，首先会加载最新的一个SNAP镜像文件，找到这个镜像文件中最大的zxid，然后再从事务日志中加载该zxid后面的数据
                 * ，同时会把从事务日志加载的数据存放到committedLog集合中
                 * 3、maxCommittedLog表示committedLog中最大议案的zxid，minCommittedLog表示committedLog中最小议案的zxid
                 */

                /**
                 *  peerLastZxid：该Learner服务器最后处理的ZXID。
                 minCommittedLog：Leader服务器提议缓存队列committedLog中的最小ZXID。
                 maxCommittedLog：Leader服务器提议缓存队列committedLog中的最大ZXID。


                 直接差异化同步（DIFF同步）
                 场景：peerLastZxid介于minCommittedLog和maxCommittedLog之间。
                 对于这个场景，就使用直接差异化同步（DIFF同步）方式即可。Leader服务器会首先向这个Learner发送一个DIFF指令，
                 用于通知Learner“进入差异化数据同步阶段，Leader服务器即将把一些Proposal同步给自己”。在实际Proposal同步过程中，
                 针对每个Proposal，Leader服务器都会通过发送两个数据包来完成，分别是PROPOSAL内容数据包和COMMIT指令数据包——这和ZooKeeper运行时Leader和Follower之间的事务请求的提交过程是一致的。

                 举个例子来说，假如某个时刻Leader服务器的提议缓存队列对应的ZXID依次是：
                 0x500000001、0x500000002、0x500000003、0x500000004、0x500000005
                 而Learner服务器最后处理的ZXID为0x500000003，于是Leader服务器就会依次将0x500000004和0x500000005两个提议同步给Learner服务器，同步过程汇总的数据包发送顺序如下

                 发送顺序	  数据包类型	  对应的ZXID
                 1	      PROPOSAL	  0x500000004
                 2	      COMMIT	    0x500000004
                 3	      PROPOSAL	  0x500000005
                 4	      COMMIT	    0x500000005

                 通过以上四个数据包的发送，Learner服务器就可以接收到自己和Leader服务器的所有差异数据。Leader服务器在发送完差异数据之后，就会将该Learner加入到forwardingFollowers或observingLearners队列中，
                 这两个队列在ZooKeeper运行期间的事务请求处理过程中都会使用到。随后Leader还会立即发送一个NEWLEADER指令，用于通知Learner，已经将提议缓存队列中的Proposal都同步给自己了。

                 下面我们再来看Learner对Leader发送过来的数据包怎么处理。根据上面讲解的Leader服务器的数据包发送顺序，Leader会首先接收到一个DIFF指令，于是便确定了接下来进入DIFF同步阶段。
                 然后依次收到上表中的四个数据包，Learner会依次将其应用到内存数据库中。紧接着，Learner还会接收到来自Leader的NEWLEADER指令，此时Learner就会反馈给Leader一个ACK消息，
                 表明自己也确实完成了对提议缓存队列中Proposal的同步。

                 Leader在接收到来自Learner的这个ACK消息以后，就认为当前Learner已经完成了数据同步，同时进入“过半策略”等待阶段——Leader会和其他Learner服务器进行上述同样的数据同步流程，
                 直到集群中有过半的Learner机器响应了Leader这个ACK消息。一旦满足“过半策略”后，Leader服务器就会向所有已经完成数据同步的Learner发送一个UPTODATE指令，用来通知Learner已经完成了数据同步，
                 同时集群中已经有过半机器完成了数据同步，集群已经具备了对外服务的能力了。

                 Learner在接收到这个来自Leader的UPTODATE指令后，会终止数据同步流程，然后向Leader再次反馈一个ACK消息。


                 先回滚再差异化同步（TRUNC+DIFF同步）
                 场景：针对上面弄的场景，我们已经介绍了直接差异化同步的详细过程。但是在这种场景中，会有一个罕见但是确实存在的特殊场景：设有A、B、C三台机器，假如某一时刻B是Leader服务器，
                 此时的Leader_Epoch为5，同时当前已经被集群中绝大部分机器都提交的ZXID包括：0x500000001和0x500000002。此时，Leader正要处理ZXID：0x500000003，
                 并且已经将该事务系如到了Leader本地的事务日志中去，就在Leader恰好要将该Proposal发送给其他Follower机器进行投票的时候，Leader服务器挂了，Proposal没有被同步出去。
                 此时ZooKeeper集群会进行新一轮的Leader选举，假设此次选举产生的新的Leader是A，同时Leader_Epoch变更为6，之后A和C两台服务器继续对外进行服务，又提交了0x600000001和0x600000002两个事务。
                 此时，服务器B再次启动，并开始数据同步。

                 简单的讲，上面这个场景就是Leader服务器在已经将事务记录到了本地事务日志中，但是没有成功发起Proposal流程的时候就挂了。
                 在这个特殊场景中，我们看到，peerLastZxid、minCommittedLog和maxCommittedLog的值分别是0x500000003、0x500000001和0x600000002，显然，peerLastZxid介于minCommittedLog和maxCommittedLog之间。

                 对于这个特殊场景，就使用先回滚再差优化同步（TRUNC+DIFF同步）的方式。当Leader服务器发现某个Learner包含了一条自己没有的事务记录，那么就需要让该Learner进行事务回滚——回滚到Leader服务器上存在的，
                 同时也是最接近于peerLastZxid的ZXID。在上面这个例子中，Leader会需要Learner回滚到ZXID为0x500000002的事务记录。

                 先回滚再差异化同步的数据同步方式在具体实现上和差异话同步是一样的，都是会将差异化的Proposal发送给Learner。同步过程中的数据包发送顺序如下表所示：

                 发送顺序	  数据包类型	  对应的ZXID
                 1	      TRUNC	      0x500000002
                 2	      PROPOSAL	  0x600000001
                 3	      COMMIT	    0x600000001
                 4	      PROPOSAL	  0x600000002
                 5	      COMMIT	    0x600000002



                 仅回滚同步（TRUNC同步）
                 场景：peerLastZxid大于maxCommittedLog。
                 这种场景其实就是上述先回滚再差异化同步的简化模式，Leader会要求Learner回滚到ZXID值为maxCommitedLog对应的事务操作。


                 全量同步（SNAP同步）
                 场景1：peerLastZxid小于minCommittedLog。
                 场景2：Leader服务器上没有提议缓存队列，peerLastZxid不等于lastProcessedZxid（Leader服务器数据恢复后得到的最大ZXID）。
                 上述这两个场景非常类似，在这两种场景下，Leader服务器都无法直接使用提议缓存队列和Learner进行数据同步，因此只能进行全量同步（SNAP同步）。

                 所谓全量同步就是Leader服务器将本机上的全量内存数据同步给Learner。Leader服务器首先向Learner发送一个SNAP指令，通知Leader即将进行全量数据同步。
                 随后，Leader会从内存数据库中获取到全量的数据节点和会话超时时间记录器，将他们序列化后传输给Learner。Learner服务器接收到该全量数据后，会对其反序列化后载入到内存数据库中。

                 以上就是ZooKeeper集群间机器的数据同步流程。整个数据同步流程的代码实现主要在LearnerHandler和Learner两个类中。

                 */
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog();
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();
                LOG.info("Synchronizing with Follower sid: " + sid
                        +" maxCommittedLog=0x"+Long.toHexString(maxCommittedLog)
                        +" minCommittedLog=0x"+Long.toHexString(minCommittedLog)
                        +" peerLastZxid=0x"+Long.toHexString(peerLastZxid));

                //获取提交的Proposal, packet的type都是Leader.PROPOSAL
                /**
                 * SNAP + committedLog 就等于ZK的全部数据
                 *
                 * ZooKeeper集群数据同步分为4类，分别为直接差异化同步(DIFF)、先回滚再差异化同步(TRUNC+DIFF)、回滚同步(TRUNC)和全量同步(SNAP)。
                 * 在同步之前,leader服务器先对peerLastZxid(该leader服务器最好处理的ZXID)、minCommittedLog、maxCommittedLog进行初始化，然后通过这3个ZXID值进行判断同步类型,并进行同步。
                 *
                 *
                 * 1、对于集群数据同步而言，通常分为四类，直接差异化同步(DIFF同步)、先回滚再差异化同步(TRUNC+DIFF同步)、仅回滚同步(TRUNC同步)、全量同步(SNAP同步)
                 * 2、Leader会优先以全量同步方式来同步数据(packetToSend默认被设置成SNAP)，然后会根据各Follower的peerLastZxid与当前Leader的peerLastZxid、minCommittedLog和maxCommittedLog进行
                 *    比较以判断Leader和Follower之间的数据差异情况来决定最终的数据同步方式：
                 * 　　  a.直接差异化同步(DIFF同步，peerLastZxid介于minCommittedLog和maxCommittedLog之间)。Leader首先向这个Learner发送一个DIFF指令，用于通知Learner进入差异化数据同步阶段，Leader即将把一些Proposal同步给自己，针对每个Proposal，Leader都会通过发送PROPOSAL内容数据包和COMMIT指令数据包来完成，
                 * 　　  b.先回滚再差异化同步(TRUNC+DIFF同步，Leader已经将事务记录到本地事务日志中，但是没有成功发起Proposal流程)。当Leader发现某个Learner包含了一条自己没有的事务记录，那么就需要该Learner进行事务回滚，回滚到Leader服务器上存在的，同时也是最接近于peerLastZxid的ZXID。
                 * 　　  c.仅回滚同步(TRUNC同步，peerLastZxid大于maxCommittedLog)。Leader要求Learner回滚到ZXID值为maxCommittedLog对应的事务操作。
                 * 　　  d.全量同步(SNAP同步，peerLastZxid小于minCommittedLog或peerLastZxid不等于lastProcessedZxid)。Leader无法直接使用提议缓存队列和Learner进行同步，因此只能进行全量同步。Leader将本机的全量内存数据同步给Learner。Leader首先向Learner发送一个SNAP指令，通知Learner即将进行全量同步，随后，Leader会从内存数据库中获取到全量的数据节点和会话超时时间记录器，将他们序列化后传输给Learner。Learner接收到该全量数据后，会对其反序列化后载入到内存数据库中。
                 */
                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog();

                //每个Learner客户端连接过来都会封装一个LearnerHandler线程单独处理Leader与该Learner的交互，peerLastZxid就是对端Learner的最大zxid
                if (peerLastZxid == leader.zk.getZKDatabase().getDataTreeLastProcessedZxid()) {
                    // Follower is already sync with us, send empty diff
                    //Learner的lastZxid等于当前Leader的lastZxid，说明Leader和Learner数据一致，不需要进行任何同步
                    LOG.info("leader and follower are in sync, zxid=0x{}",
                            Long.toHexString(peerLastZxid));
                    packetToSend = Leader.DIFF;
                    //如果learner已经是同步的了，也发送DIFF，只是发送的zxidToSend和learner本地一样，相当于空的DIFF
                    zxidToSend = peerLastZxid;
                } else if (proposals.size() != 0) {
                    LOG.debug("proposal size is {}", proposals.size());
                    if ((maxCommittedLog >= peerLastZxid)
                            && (minCommittedLog <= peerLastZxid)) {
                        /**
                         * 1、如果learner的zxid在leader的[minCommittedLog, maxCommittedLog]范围内，
                         *    这种情况说明Learner和Leader之间的数据差异较小，采用差异化同步(DIFF)
                         * 2、首先，遍历committedLog集合，查找到Learner没有同步的议案，判断的方式是根据zxid，由于zxid是顺序递增的，
                         *    committedLog存放的议案也是顺序的
                         * 3、这里，存在一种特殊情况：Learner存在的议案在Leader的committedLog集合中不存在，这时就要先回滚(TRUNC)Learner，
                         *    然后在进行差异化同步(DIFF)，即TRUNC+DIFF方式
                         */
                        LOG.debug("Sending proposals to follower");

                        // as we look through proposals, this variable keeps track of previous
                        // proposal Id.
                        long prevProposalZxid = minCommittedLog;

                        // Keep track of whether we are about to send the first packet.
                        // Before sending the first packet, we have to tell the learner
                        // whether to expect a trunc or a diff
                        boolean firstPacket=true;

                        // If we are here, we can use committedLog to sync with
                        // follower. Then we only need to decide whether to
                        // send trunc or not
                        packetToSend = Leader.DIFF;//默认是DIFF操作
                        zxidToSend = maxCommittedLog;

                        for (Proposal propose: proposals) {
                            // skip the proposals the peer already has
                            if (propose.packet.getZxid() <= peerLastZxid) {//leader提交的proposal已经被learner处理过了，那么就跳过
                                prevProposalZxid = propose.packet.getZxid();
                                continue;
                            } else {
                                //该议案没有被Learner处理过
                                // If we are sending the first packet, figure out whether to trunc
                                // in case the follower has some proposals that the leader doesn't
                                if (firstPacket) {//第一个发送的packet
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) {//如果learner有一些leader不知道的请求(正常来说应该是prevProposalZxid == peerLastZxid)
                                        /**
                                         * 1、正常来说应该是prevProposalZxid == peerLastZxid，即prevProposalZxid后面的议案都需要同步到对端Learner
                                         * 2、prevProposalZxid < peerLastZxid说明对端存在一些议案当前Leader上没有同步的，这时，首先对对端Learner进行
                                         *    一次回滚(TRUNC)到prevProposalZxid操作
                                         */
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC;  //让learner回滚
                                        zxidToSend = prevProposalZxid;
                                        updates = zxidToSend;//让learner回滚
                                    }
                                }
                                //将差异的议案放入到发送队列中，等待发送到对端进行数据同步，注意：这里只是放入到队列中，并不会真正发送出去
                                queuePacket(propose.packet);
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(),
                                        null, null);
                                //同理，每个议案都会对应一个commit议案，让Learner真正执行事务提交，将数据变更合入到内存DataTree上客户端即可见
                                queuePacket(qcommit);
                            }
                        }
                    } else if (peerLastZxid > maxCommittedLog) {//learner的zxid比leader的大，让learner回滚
                        LOG.debug("Sending TRUNC to follower zxidToSend=0x{} updates=0x{}",
                                Long.toHexString(maxCommittedLog),
                                Long.toHexString(updates));

                        packetToSend = Leader.TRUNC;
                        zxidToSend = maxCommittedLog;
                        updates = zxidToSend;
                    } else {
                        //Learner的peerLastZxid小于minCommittedLog，表明Learner和Leader的数据差异较大，
                        //应该采用全量数据同步(SNAP)，这里不进行任何操作是因为默认就是采用SNAP方式同步
                        LOG.warn("Unhandled proposal scenario");
                    }
                } else {//committedLog议案集合为空情况，没法进行差异化比对，因此也只能进行全量数据同步(SNAP)
                    // just let the state transfer happen
                    LOG.debug("proposals is empty");
                }               

                LOG.info("Sending " + Leader.getPacketType(packetToSend));
                leaderLastZxid = leader.startForwarding(this, updates);

            } finally {
                rl.unlock();
            }

            //生成NEWLEADER的packet,发给learner代表自己需要同步的信息发完了
             QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                    ZxidUtils.makeZxid(newEpoch, 0), null, null);
             if (getVersion() < 0x10000) {
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                queuedPackets.add(newLeaderQP);//加入发送队列
            }
            bufferedOutput.flush();//发送NEWLEADER消息
            //Need to set the zxidToSend to the latest zxid
            if (packetToSend == Leader.SNAP) {//如果是SNAP同步，获取zxid
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            //告诉learner如何同步，这里会被真实的发送到网络IO
            oa.writeRecord(new QuorumPacket(packetToSend, zxidToSend, null, null), "packet");
            bufferedOutput.flush();
            
            /* if we are not truncating or sending a diff just send a snapshot */
            //如果发出snap，代表告知learner进行snap方式的数据同步
            if (packetToSend == Leader.SNAP) {
                LOG.info("Sending snapshot last zxid of peer is 0x"
                        + Long.toHexString(peerLastZxid) + " " 
                        + " zxid of leader is 0x"
                        + Long.toHexString(leaderLastZxid)
                        + "sent zxid of db as 0x" 
                        + Long.toHexString(zxidToSend));
                // Dump data to peer
                //SNAP恢复就是把当前的db的序列化内容发送出去
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                //有特定的签名
                oa.writeString("BenWasHere", "signature");
            }
            bufferedOutput.flush();
            
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();//不断发送packets直到接受到proposalOfDeath
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption",e);
                    }
                }
            }.start();//启动线程，发送消息
            
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            if(qp.getType() != Leader.ACK){//Learner接收到NEWLEADER 一定会返回ACK
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            LOG.info("Received NEWLEADER-ACK message from " + getSid());
            /**
             * waitForNewLeaderAck会阻塞，直到有过半参与者返回NEWLEADER的Ack消息
             * 返回NEWLEADER的Ack表示当前代表的Learner同步完成
             *
             * 同步时发送同步命令(DIFF+SNAP+TRUNC)、然后发送同步Proposal+Committed、再发送NewLeader表示同步数据发送完成、最后对端接收到NewLeader后进行返回Ack
             */
            leader.waitForNewLeaderAck(getSid(), qp.getZxid(), getLearnerType());
            //开始同步超时检测
            syncLimitCheck.start();
            
            // now that the ack has been processed expect the syncLimit
            //请求阶段的读取超时时间 为 tickTime * syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);

            //等待直到leader启动完成
            synchronized(leader.zk){
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            //发送update代表过半的机器回复了NEWLEADER的ACK
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            /**
             * 第五步、Leader和Learner数据同步完成，即新选出的Leader所有前期准备工作全部完成，开始真正进入处理请求阶段
             *
             *        下面通过一个无限循环方式不停的接收Follower端的数据包进行处理，我们知道Follower节点可以处理客户端的非事务性请求，
             *        而对于事务性请求，Follower节点会把请求转发给Leader节点进行处理，下面的REQUEST分支就是用于处理Follower端转发给Leader客户端请求的逻辑,
             *        把Follower发送过来的数据包封装成一个Request请求，调用LeaderZooKeeperServer.submitRequest()通过“责任链”方式处理该Request请求
             */
            while (true) {//正常交互，处理learner的请求等
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }

                /**
                 * 1、leader.self.tick可以认为是zk的一个时钟，Leader会周期的向集群中所有节点发送ping数据包，每次周期leader.self.tick累加1
                 * 2、tickOfNextAckDeadline可以看成是一个类似时间戳的东西，从当前到这个时间戳之间如果Leader没有接收到任何该Learner服务器的数据包，
                 *    Leader就会认为该Learner节点已经故障断开，在统计正常节点时就不会计算在内(Leader会周期统计正常节点数，当正常节点数小于集群一半就认为整个集群存在故障，
                 *    即会进入Leader选举状态，对外服务就无法继续工作，具体可以参见Leader中代码)
                 * 3、tickOfNextAckDeadline作用就是判断当Leader与该Learner间有leader.self.syncLimit个周期没有任何网络IO通信，即认为该Learner节点故障
                 */
                tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    syncLimitCheck.updateAck(qp.getZxid());
                    //更新proposal对应的ack时间
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp
                            .getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        leader.zk.touch(sess, to);//会话管理，激活
                    }
                    break;
                case Leader.REVALIDATE:
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.touch(id, to);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);//设置owner是当前learnerHandler
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    //返回是否valid
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);
                    break;
                case Leader.REQUEST:                    
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    //提交请求
                    leader.zk.submitRequest(si);
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the 
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " 
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }
    //关闭当前handler，sock，以及让sendPackets的异步线程最终停止
    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    //发送ping命令，本质就是检测leader与learner是否有proposal超时了(超过指定时长没有收到ack)

    /**
     * 如果Leader发送出去的Leader.PROPOSAL请求在一段时间内（这个时间由 conf/zoo.cfg 中的 syncLimit 决定）没有收到对应的ACK，就会导致syncLimitCheck.check失败，
     * 从而调用LearnerHandler.shutdown关闭到这个Follower的连接，并停止对应的发送、接收请求的线程。
     *
     * 在 Follower 这边，由于 Leader 连接关闭，调用 Learner.readPacket 时会抛出异常，退出 Follower.followLeader 方法，重新进入 LOOKING 状态。
     *
     * 综上，我们知道了，发送到某个 Follower 的 Proposal 请求被丢弃，会导致对应的 Follower 重新进入 LOOKING 状态。
     */
    public void ping() {
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {//如果还没有超时
            synchronized(leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {//如果已经超时，就关闭这个handler
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();//关闭当前handler，sock，以及让syncLimitCheck任务最终停止
        }
    }
    //将packet加入异步发送队列
    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }
    //是否保持同步

    /**
     * 接收到Follower的任何消息，都会对tickOfNextAckDeadline执行tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit
     * Leader执行一次ping()就会对leader.self.tick+1，leader.self.tick可以看着系统的一个时钟
     * leader.self.tick.get() <= tickOfNextAckDeadline说明很久没有接收到该Follower的数据包了，即超时了
     */
    public boolean synced() {
        return isAlive()
        && leader.self.tick.get() <= tickOfNextAckDeadline;//线程活着，且当前周期数<deadline
    }
}
