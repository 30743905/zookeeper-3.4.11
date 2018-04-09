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

import java.io.IOException;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.DataTreeBean;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * 
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: PrepRequestProcessor -> ProposalRequestProcessor ->
 * CommitProcessor -> Leader.ToBeAppliedRequestProcessor ->
 * FinalRequestProcessor
 *
 * leaderZookeeperServer
    Leader要完成以下几个事情：
        1、接收客户端的request请求
        2、将会修改同步数据的request请求 转化为proposal，并保存.
        3、向所有的follower发送proposal。
        4、接收follower的ack。
        5、统计收到的ack，如果某一个proposal的ack超过了半数，那么向所有follower发送commit 信令，并向所有observer发送inform信令，执行这个proposal的动作。
        6、leader自己执行已经被commit的proposal所对应的操作，并回复结果。

 LeaderZooKeeperServer继承QuorumZooKeeperServer抽象类，其会继承ZooKeeperServer中的很多方法
 */
public class LeaderZooKeeperServer extends QuorumZooKeeperServer {
    //提交请求处理器
    //其只有一个CommitProcessor类，表示提交请求处理器，其在处理链中的位置位于ProposalRequestProcessor之后，ToBeAppliedRequestProcessor之前
    CommitProcessor commitProcessor;

    /**
     * @param port
     * @param dataDir
     * @throws IOException
     */
    LeaderZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, treeBuilder, zkDb, self);
    }

    public Leader getLeader(){
        return self.leader;
    }

    /**
     * 这段代码其实内容很简单，却非常重要。这里采用了类似于“责任链”的设计模式，该函数的作用就是把处理请求的逻辑链建立起来。
     * “责任链”的确是建立起来了，那么这条链是何时被触发的呢？LeaderZooKeeperServer是QuorumZooKeeperServer的直接子类。
     * 该类有一个QuorumPeer类型的成员self，而self有有一个Leader类型的成员leader。这个leader类型的变量完成了网络交互方面的工作，
     * 也是触发前文中“责任链”的地方。
     *
     * 该函数表示创建处理链，可以看到其处理链的顺序为:
     * PrepRequestProcessor -> ProposalRequestProcessor -> CommitProcessor -> Leader.ToBeAppliedRequestProcessor -> FinalRequestProcessor
     */
    @Override
    protected void setupRequestProcessors() {
        //创建FinalRequestProcessor
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        // 创建ToBeAppliedRequestProcessor
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(
                finalProcessor, getLeader().toBeApplied);
        // 创建CommitProcessor
        commitProcessor = new CommitProcessor(toBeAppliedProcessor,
                Long.toString(getServerId()), false,
                getZooKeeperServerListener());
        // 启动CommitProcessor
        commitProcessor.start();
        // 创建ProposalRequestProcessor
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(this,
                commitProcessor);
        // 初始化ProposalProcessor
        proposalProcessor.initialize();
        // firstProcessor为PrepRequestProcessor
        firstProcessor = new PrepRequestProcessor(this, proposalProcessor);
        // 启动PrepRequestProcessor
        ((PrepRequestProcessor)firstProcessor).start();
    }

    @Override
    public int getGlobalOutstandingLimit() {
        return super.getGlobalOutstandingLimit() / (self.getQuorumSize() - 1);
    }
    
    @Override
    public void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(this, getZKDatabase()
                .getSessionWithTimeOuts(), tickTime, self.getId(),
                getZooKeeperServerListener());
    }
    
    @Override
    protected void startSessionTracker() {
        ((SessionTrackerImpl)sessionTracker).start();
    }


    public boolean touch(long sess, int to) {
        return sessionTracker.touchSession(sess, to);
    }

    /**
     * 该函数用于注册JMX服务，首先使用DataTree初始化DataTreeBean，然后使用DataTreeBean和ServerBean调用register函数进行注册
     */
    @Override
    protected void registerJMX() {
        // register with JMX
        try {
            jmxDataTreeBean = new DataTreeBean(getZKDatabase().getDataTree());
            MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxDataTreeBean = null;
        }
    }

    public void registerJMX(LeaderBean leaderBean,
            LocalPeerBean localPeerBean)
    {
        // register with JMX
        if (self.jmxLeaderElectionBean != null) {
            try {
                MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }

        try {
            jmxServerBean = leaderBean;
            MBeanRegistry.getInstance().register(leaderBean, localPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    //该函数用于取消注册JMX服务，其会调用unregister函数
    @Override
    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxDataTreeBean = null;
    }

    protected void unregisterJMX(Leader leader) {
        // unregister from JMX
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
    }
    
    @Override
    public String getState() {
        return "leader";
    }

    /**
     * Returns the id of the associated QuorumPeer, which will do for a unique
     * id of this server. 
     */
    @Override
    public long getServerId() {
        return self.getId();
    }    
    
    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
        int sessionTimeout) throws IOException {
        super.revalidateSession(cnxn, sessionId, sessionTimeout);
        try {
            // setowner as the leader itself, unless updated
            // via the follower handlers
            setOwner(sessionId, ServerCnxn.me);
        } catch (SessionExpiredException e) {
            // this is ok, it just means that the session revalidation failed.
        }
    }
}
