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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetSASLResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.ServerCnxn.CloseRequestException;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a simple standalone ZooKeeperServer. It sets up the
 * following chain of RequestProcessors to process requests:
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
 *
 * ZookeeperServer，顾名思义就是一个Zookeeper节点的的服务端，为处理请求，建立了一个如下的处理链：
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor。
 *
 * SyncRequestProcessor：该线程将请求作为日志写到磁盘中，当请求累计到一定数量时，写到磁盘，且只有当请求都写到磁盘后，请求才会传递到下一个处理环节。
 */
public class ZooKeeperServer implements SessionExpirer, ServerStats.Provider {
    protected static final Logger LOG;
    
    static {
        LOG = LoggerFactory.getLogger(ZooKeeperServer.class);
        
        Environment.logEnv("Server environment:", LOG);
    }

    protected ZooKeeperServerBean jmxServerBean;
    protected DataTreeBean jmxDataTreeBean;

 
    /**
     * The server delegates loading of the tree to an instance of the interface
     */
    public interface DataTreeBuilder {
        public DataTree build();
    }

    static public class BasicDataTreeBuilder implements DataTreeBuilder {
        public DataTree build() {
            return new DataTree();
        }
    }

    public static final int DEFAULT_TICK_TIME = 3000;
    protected int tickTime = DEFAULT_TICK_TIME;
    /** value of -1 indicates unset, use default */
    protected int minSessionTimeout = -1;
    /** value of -1 indicates unset, use default */
    protected int maxSessionTimeout = -1;
    protected SessionTracker sessionTracker;
    private FileTxnSnapLog txnLogFactory = null;
    private ZKDatabase zkDb;
    private final AtomicLong hzxid = new AtomicLong(0);
    public final static Exception ok = new Exception("No prob");
    protected RequestProcessor firstProcessor;
    protected volatile State state = State.INITIAL;

    protected enum State {
        INITIAL, RUNNING, SHUTDOWN, ERROR;
    }

    /**
     * This is the secret that we use to generate passwords, for the moment it
     * is more of a sanity check.
     */
    static final private long superSecret = 0XB3415C00L;

    private final AtomicInteger requestsInProcess = new AtomicInteger(0);
    final List<ChangeRecord> outstandingChanges = new ArrayList<ChangeRecord>();
    // this data structure must be accessed under the outstandingChanges lock
    final HashMap<String, ChangeRecord> outstandingChangesForPath =
        new HashMap<String, ChangeRecord>();
    
    private ServerCnxnFactory serverCnxnFactory;

    private final ServerStats serverStats;
    private final ZooKeeperServerListener listener;
    private ZooKeeperServerShutdownHandler zkShutdownHandler;

    void removeCnxn(ServerCnxn cnxn) {
        zkDb.removeCnxn(cnxn);
    }
 
    /**
     * Creates a ZooKeeperServer instance. Nothing is setup, use the setX
     * methods to prepare the instance (eg datadir, datalogdir, ticktime, 
     * builder, etc...)
     * 
     * @throws IOException
     */
    public ZooKeeperServer() {
        serverStats = new ServerStats(this);
        listener = new ZooKeeperServerListenerImpl(this);
    }
    
    /**
     * Creates a ZooKeeperServer instance. It sets everything up, but doesn't
     * actually start listening for clients until run() is invoked.
     * 
     * @param dataDir the directory to put the data
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime,
            int minSessionTimeout, int maxSessionTimeout,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) {
        serverStats = new ServerStats(this);
        this.txnLogFactory = txnLogFactory;
        this.zkDb = zkDb;
        this.tickTime = tickTime;
        this.minSessionTimeout = minSessionTimeout;
        this.maxSessionTimeout = maxSessionTimeout;

        listener = new ZooKeeperServerListenerImpl(this);

        LOG.info("Created server with tickTime " + tickTime
                + " minSessionTimeout " + getMinSessionTimeout()
                + " maxSessionTimeout " + getMaxSessionTimeout()
                + " datadir " + txnLogFactory.getDataDir()
                + " snapdir " + txnLogFactory.getSnapDir());
    }

    /**
     * creates a zookeeperserver instance. 
     * @param txnLogFactory the file transaction snapshot logging class
     * @param tickTime the ticktime for the server
     * @param treeBuilder the datatree builder
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime,
            DataTreeBuilder treeBuilder) throws IOException {
        this(txnLogFactory, tickTime, -1, -1, treeBuilder,
                new ZKDatabase(txnLogFactory));
    }
    
    public ServerStats serverStats() {
        return serverStats;
    }

    public void dumpConf(PrintWriter pwriter) {
        pwriter.print("clientPort=");
        pwriter.println(getClientPort());
        pwriter.print("dataDir=");
        pwriter.println(zkDb.snapLog.getSnapDir().getAbsolutePath());
        pwriter.print("dataLogDir=");
        pwriter.println(zkDb.snapLog.getDataDir().getAbsolutePath());
        pwriter.print("tickTime=");
        pwriter.println(getTickTime());
        pwriter.print("maxClientCnxns=");
        pwriter.println(serverCnxnFactory.getMaxClientCnxnsPerHost());
        pwriter.print("minSessionTimeout=");
        pwriter.println(getMinSessionTimeout());
        pwriter.print("maxSessionTimeout=");
        pwriter.println(getMaxSessionTimeout());

        pwriter.print("serverId=");
        pwriter.println(getServerId());
    }

    /**
     * This constructor is for backward compatibility with the existing unit
     * test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public ZooKeeperServer(File snapDir, File logDir, int tickTime)
            throws IOException {
        this( new FileTxnSnapLog(snapDir, logDir),
                tickTime, new BasicDataTreeBuilder());
    }

    /**
     * Default constructor, relies on the config for its agrument values
     *
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory,
            DataTreeBuilder treeBuilder)
        throws IOException
    {
        this(txnLogFactory, DEFAULT_TICK_TIME, -1, -1, treeBuilder,
                new ZKDatabase(txnLogFactory));
    }

    /**
     * get the zookeeper database for this server
     * @return the zookeeper database for this server
     */
    public ZKDatabase getZKDatabase() {
        return this.zkDb;
    }
    
    /**
     * set the zkdatabase for this zookeeper server
     * @param zkDb
     */
    public void setZKDatabase(ZKDatabase zkDb) {
       this.zkDb = zkDb;
    }
    
    /**
     *  Restore sessions and data
     *  该函数用于加载数据，其首先会判断内存库是否已经加载设置zxid，之后会调用killSession函数删除过期的会话，
     *  killSession会从sessionTracker中删除session，并且killSession最后会调用DataTree的killSession函数　
     *
     *  恢复session和数据，单机版启动或者集群版leader选举之后调用lead方法时，会调用该方法。主要完成设置zxid以及把无效的session给kill掉的工作
     */
    public void loadData() throws IOException, InterruptedException {
        /*
         * When a new leader starts executing Leader#lead, it 
         * invokes this method. The database, however, has been
         * initialized before running leader election so that
         * the server could pick its zxid for its initial vote.
         * It does it by invoking QuorumPeer#getLastLoggedZxid.
         * Consequently, we don't need to initialize it once more
         * and avoid the penalty of loading it a second time. Not 
         * reloading it is particularly important for applications
         * that host a large database.
         * 
         * The following if block checks whether the database has
         * been initialized or not. Note that this method is
         * invoked by at least one other method: 
         * ZooKeeperServer#startdata.
         *  
         * See ZOOKEEPER-1642 for more detail.
         */
        if(zkDb.isInitialized()){
            setZxid(zkDb.getDataTreeLastProcessedZxid());// 设置为最后处理的Zxid
        }
        else {// 未被初始化，则加载数据库
            setZxid(zkDb.loadDataBase());
        }
        
        // Clean up dead sessions
        LinkedList<Long> deadSessions = new LinkedList<Long>();
        for (Long session : zkDb.getSessions()) {// 遍历所有的会话
            if (zkDb.getSessionWithTimeOuts().get(session) == null) {// 删除过期的会话
                deadSessions.add(session);
            }
        }
        zkDb.setDataTreeInit(true);// 完成DataTree的初始化
        for (long session : deadSessions) {// 遍历过期会话
            // XXX: Is lastProcessedZxid really the best thing to use?
            killSession(session, zkDb.getDataTreeLastProcessedZxid());// 删除会话
        }
    }

    public void takeSnapshot(){

        try {
            txnLogFactory.save(zkDb.getDataTree(), zkDb.getSessionWithTimeOuts());
        } catch (IOException e) {
            LOG.error("Severe unrecoverable error, exiting", e);
            // This is a severe error that we cannot recover from,
            // so we need to exit
            System.exit(10);
        }
    }

  
    /**
     * This should be called from a synchronized block on this!
     */
    public long getZxid() {
        return hzxid.get();
    }

    //获取下一个server的zxid,调用方需要确保控制并发顺序
    long getNextZxid() {
        return hzxid.incrementAndGet();
    }

    public void setZxid(long zxid) {
        hzxid.set(zxid);
    }

    //上面ZooKeeperServer#expire调用了close函数，介绍如下
    //该函数用于提交一个 关闭某个sessionId 的请求
    private void close(long sessionId) {
        submitRequest(null, sessionId, OpCode.closeSession, 0, null, null);
    }
    
    public void closeSession(long sessionId) {
        LOG.info("Closing session 0x" + Long.toHexString(sessionId));
        
        // we do not want to wait for a session close. send it as soon as we
        // detect it!
        close(sessionId);
    }

    /**
     * DataTree的killSession函数的逻辑首先移除session，然后取得该session下的所有临时节点，然后逐一删除临时节点
     */
    protected void killSession(long sessionId, long zxid) {
        zkDb.killSession(sessionId, zxid);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                                         "ZooKeeperServer --- killSession: 0x"
                    + Long.toHexString(sessionId));
        }
        if (sessionTracker != null) {
            sessionTracker.removeSession(sessionId);
        }
    }

    public void expire(Session session) {
        long sessionId = session.getSessionId();
        LOG.info("Expiring session 0x" + Long.toHexString(sessionId)
                + ", timeout of " + session.getTimeout() + "ms exceeded");
        close(sessionId);
    }

    public static class MissingSessionException extends IOException {
        private static final long serialVersionUID = 7467414635467261007L;

        public MissingSessionException(String msg) {
            super(msg);
        }
    }
    
    void touch(ServerCnxn cnxn) throws MissingSessionException {
        if (cnxn == null) {
            return;
        }
        long id = cnxn.getSessionId();
        int to = cnxn.getSessionTimeout();
        if (!sessionTracker.touchSession(id, to)) {
            throw new MissingSessionException(
                    "No session with sessionid 0x" + Long.toHexString(id)
                    + " exists, probably expired and removed");
        }
    }

    protected void registerJMX() {
        // register with JMX
        try {
            jmxServerBean = new ZooKeeperServerBean(this);
            MBeanRegistry.getInstance().register(jmxServerBean, null);
            
            try {
                jmxDataTreeBean = new DataTreeBean(zkDb.getDataTree());
                MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
                jmxDataTreeBean = null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }
    
    public void startdata() 
    throws IOException, InterruptedException {
        //check to see if zkDb is not null
        if (zkDb == null) {
            zkDb = new ZKDatabase(this.txnLogFactory);
        }  
        if (!zkDb.isInitialized()) {
            loadData();
        }
    }

    //入口是ZooKeeperServer#startup，zkServer都是在上述加载了db的数据之后，调用startup来完成启动
    public synchronized void startup() {
        if (sessionTracker == null) {
            createSessionTracker();//创建回话跟踪器
        }
        startSessionTracker();//启动会话管理
        setupRequestProcessors();//初始化请求处理链路

        registerJMX();//注册jmx

        setState(State.RUNNING);//提供服务
        notifyAll();
    }
    //安装请求处理链路,是PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor顺序
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this,
                finalProcessor);
        ((SyncRequestProcessor)syncProcessor).start();
        //链路是PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor顺序
        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
        ((PrepRequestProcessor)firstProcessor).start();//启动链路
    }

    public ZooKeeperServerListener getZooKeeperServerListener() {
        return listener;
    }

    protected void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(this, zkDb.getSessionWithTimeOuts(),
                tickTime, 1, getZooKeeperServerListener());//1是默认单机版实现的sid
        //这里是默认的单机版实现，在集群版不同的角色有不同的实现,主要是参数sid不会传1，而是配置中的sid
    }
    
    protected void startSessionTracker() {
        ((SessionTrackerImpl)sessionTracker).start();
    }

    /**
     * Sets the state of ZooKeeper server. After changing the state, it notifies
     * the server state change to a registered shutdown handler, if any.
     * <p>
     * The following are the server state transitions:
     * <li>During startup the server will be in the INITIAL state.</li>
     * <li>After successfully starting, the server sets the state to RUNNING.
     * </li>
     * <li>The server transitions to the ERROR state if it hits an internal
     * error. {@link ZooKeeperServerListenerImpl} notifies any critical resource
     * error events, e.g., SyncRequestProcessor not being able to write a txn to
     * disk.</li>
     * <li>During shutdown the server sets the state to SHUTDOWN, which
     * corresponds to the server not running.</li>
     *
     * @param state new server state.
     */
    protected void setState(State state) {
        this.state = state;
        // Notify server state changes to the registered shutdown handler, if any.
        if (zkShutdownHandler != null) {
            zkShutdownHandler.handle(state);
        } else {
            LOG.error("ZKShutdownHandler is not registered, so ZooKeeper server "
                    + "won't take any action on ERROR or SHUTDOWN server state changes");
        }
    }

    /**
     * This can be used while shutting down the server to see whether the server
     * is already shutdown or not.
     *
     * @return true if the server is running or server hits an error, false
     *         otherwise.
     */
    protected boolean canShutdown() {
        return state == State.RUNNING || state == State.ERROR;
    }

    public boolean isRunning() {
        return state == State.RUNNING;
    }

    public void shutdown() {
        shutdown(false);
    }

    /**
     * Shut down the server instance
     * @param fullyShutDown true if another server using the same database will not replace this one in the same process
     */
    public synchronized void shutdown(boolean fullyShutDown) {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("shutting down");

        // new RuntimeException("Calling shutdown").printStackTrace();
        setState(State.SHUTDOWN);
        // Since sessionTracker and syncThreads poll we just have to
        // set running to false and they will detect it during the poll
        // interval.
        if (sessionTracker != null) {
            sessionTracker.shutdown();
        }
        if (firstProcessor != null) {
            firstProcessor.shutdown();
        }

        if (fullyShutDown && zkDb != null) {
            zkDb.clear();
        }
        // else there is no need to clear the database
        //  * When a new quorum is established we can still apply the diff
        //    on top of the same zkDb data
        //  * If we fetch a new snapshot from leader, the zkDb will be
        //    cleared anyway before loading the snapshot

        unregisterJMX();
    }

    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
        jmxDataTreeBean = null;
    }

    public void incInProcess() {
        requestsInProcess.incrementAndGet();
    }

    public void decInProcess() {
        requestsInProcess.decrementAndGet();
    }

    public int getInProcess() {
        return requestsInProcess.get();
    }

    /**
     * This structure is used to facilitate information sharing between PrepRP
     * and FinalRP.
     */
    static class ChangeRecord {
        ChangeRecord(long zxid, String path, StatPersisted stat, int childCount,
                List<ACL> acl) {
            this.zxid = zxid;
            this.path = path;
            this.stat = stat;
            this.childCount = childCount;
            this.acl = acl;
        }

        long zxid;

        String path;

        StatPersisted stat; /* Make sure to create a new object when changing */

        int childCount;

        List<ACL> acl; /* Make sure to create a new object when changing */

        @SuppressWarnings("unchecked")
        ChangeRecord duplicate(long zxid) {
            StatPersisted stat = new StatPersisted();
            if (this.stat != null) {
                DataTree.copyStatPersisted(this.stat, stat);
            }
            return new ChangeRecord(zxid, path, stat, childCount,
                    acl == null ? new ArrayList<ACL>() : new ArrayList<ACL>(acl));
        }
    }
    //根据sessionId生成密码
    byte[] generatePasswd(long id) {
        Random r = new Random(id ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        return p;
    }

    protected boolean checkPasswd(long sessionId, byte[] passwd) {
        return sessionId != 0
                && Arrays.equals(passwd, generatePasswd(sessionId));
    }

    long createSession(ServerCnxn cnxn, byte passwd[], int timeout) {
        /**
         * sessionTracker是在LeaderZookeeper.createSessionTracker()，默认实现类：SessionTrackerImpl
         */
        long sessionId = sessionTracker.createSession(timeout);
        Random r = new Random(sessionId ^ superSecret);
        r.nextBytes(passwd);
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(timeout);
        cnxn.setSessionId(sessionId);
        submitRequest(cnxn, sessionId, OpCode.createSession, 0, to, null);
        return sessionId;
    }

    /**
     * set the owner of this session as owner
     * @param id the session id
     * @param owner the owner of the session
     * @throws SessionExpiredException
     */
    public void setOwner(long id, Object owner) throws SessionExpiredException {
        sessionTracker.setOwner(id, owner);
    }

    //在会话跟踪器SessionTracker中判断会话是否还有效
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
            int sessionTimeout) throws IOException {
        boolean rc = sessionTracker.touchSession(sessionId, sessionTimeout);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                                     "Session 0x" + Long.toHexString(sessionId) +
                    " is valid: " + rc);
        }
        finishSessionInit(cnxn, rc);
    }

    public void reopenSession(ServerCnxn cnxn, long sessionId, byte[] passwd,
            int sessionTimeout) throws IOException {
        if (!checkPasswd(sessionId, passwd)) {//如果密码不对
            finishSessionInit(cnxn, false);//完成初始化，传入valid值为false
        } else {
            revalidateSession(cnxn, sessionId, sessionTimeout);//如果密码正确，判断会话跟踪器的记录
        }
    }

    //完成会话初始化，根据参数valid代表认证通过与否，用来判断server是接收连接请求，还是发出closeConn的请求
    public void finishSessionInit(ServerCnxn cnxn, boolean valid) {
        // register with JMX
        try {
            if (valid) {
                serverCnxnFactory.registerConnection(cnxn);
            }
        } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
        }

        try {
            ConnectResponse rsp = new ConnectResponse(0, valid ? cnxn.getSessionTimeout()
                    : 0, valid ? cnxn.getSessionId() : 0, // send 0 if session is no
                            // longer valid
                            valid ? generatePasswd(cnxn.getSessionId()) : new byte[16]);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            bos.writeInt(-1, "len");
            rsp.serialize(bos, "connect");
            if (!cnxn.isOldClient) {
                bos.writeBool(
                        this instanceof ReadOnlyZooKeeperServer, "readOnly");
            }
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.putInt(bb.remaining() - 4).rewind();
            cnxn.sendBuffer(bb);    

            if (!valid) {
                LOG.info("Invalid session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " for client "
                        + cnxn.getRemoteSocketAddress()
                        + ", probably expired");
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
            } else {
                LOG.info("Established session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " with negotiated timeout " + cnxn.getSessionTimeout()
                        + " for client "
                        + cnxn.getRemoteSocketAddress());
                cnxn.enableRecv();
            }
                
        } catch (Exception e) {
            LOG.warn("Exception while establishing session, closing", e);
            cnxn.close();
        }
    }
    
    public void closeSession(ServerCnxn cnxn, RequestHeader requestHeader) {
        closeSession(cnxn.getSessionId());
    }

    public long getServerId() {
        return 0;
    }

    /**
     * @param cnxn
     * @param sessionId
     * @param xid
     * @param bb
     */
    private void submitRequest(ServerCnxn cnxn, long sessionId, int type,
            int xid, ByteBuffer bb, List<Id> authInfo) {
        Request si = new Request(cnxn, sessionId, xid, type, bb, authInfo);
        submitRequest(si);
    }

    /**
     * 当firstProcessor为空时，并且running标志为false时，其会一直等待，直到state为INITIAL，
     * 之后调用touch函数判断session是否存在或者已经超时，之后判断请求的类型是否合法，合法则使用请求处理器进行处理
     */
    public void submitRequest(Request si) {
        if (firstProcessor == null) {// 第一个处理器为空,等待第一个处理器构造完成
            synchronized (this) {
                try {
                    // Since all requests are passed to the request
                    // processor it should wait for setting up the request
                    // processor chain. The state will be updated to RUNNING
                    // after the setup.
                    while (state == State.INITIAL) {// 直到state为INITIAL，否则继续等待
                        wait(1000);
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected interruption", e);
                }
                if (firstProcessor == null || state != State.RUNNING) {
                    throw new RuntimeException("Not started");
                }
            }
        }
        try {
            //更新SessionTracker相关统计
            touch(si.cnxn);
            // 是否为合法的packet
            boolean validpacket = Request.isValid(si.type);
            if (validpacket) {
                //支持的type就调用第一个处理链来处理
                firstProcessor.processRequest(si);
                if (si.cnxn != null) {
                    incInProcess();//正在处理的请求个数 +1
                }
            } else {
                LOG.warn("Received packet at server of unknown type " + si.type);
                new UnimplementedRequestProcessor().processRequest(si);
            }
        } catch (MissingSessionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping request: " + e.getMessage());
            }
        } catch (RequestProcessorException e) {
            LOG.error("Unable to process request:" + e.getMessage(), e);
        }
    }

    public static int getSnapCount() {
        String sc = System.getProperty("zookeeper.snapCount");
        try {
            int snapCount = Integer.parseInt(sc);

            // snapCount must be 2 or more. See org.apache.zookeeper.server.SyncRequestProcessor
            if( snapCount < 2 ) {
                LOG.warn("SnapCount should be 2 or more. Now, snapCount is reset to 2");
                snapCount = 2;
            }
            return snapCount;
        } catch (Exception e) {
            return 100000;
        }
    }

    public int getGlobalOutstandingLimit() {
        String sc = System.getProperty("zookeeper.globalOutstandingLimit");
        int limit;
        try {
            limit = Integer.parseInt(sc);
        } catch (Exception e) {
            limit = 1000;
        }
        return limit;
    }

    public void setServerCnxnFactory(ServerCnxnFactory factory) {
        serverCnxnFactory = factory;
    }

    public ServerCnxnFactory getServerCnxnFactory() {
        return serverCnxnFactory;
    }

    /**
     * return the last proceesed id from the 
     * datatree
     */
    public long getLastProcessedZxid() {
        return zkDb.getDataTreeLastProcessedZxid();
    }

    /**
     * return the outstanding requests
     * in the queue, which havent been 
     * processed yet
     */
    public long getOutstandingRequests() {
        return getInProcess();
    }

    /**
     * trunccate the log to get in sync with others 
     * if in a quorum
     * @param zxid the zxid that it needs to get in sync
     * with others
     * @throws IOException
     */
    public void truncateLog(long zxid) throws IOException {
        this.zkDb.truncateLog(zxid);
    }
       
    public int getTickTime() {
        return tickTime;
    }

    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
    }

    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;
    }

    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    public int getClientPort() {
        return serverCnxnFactory != null ? serverCnxnFactory.getLocalPort() : -1;
    }

    public void setTxnLogFactory(FileTxnSnapLog txnLog) {
        this.txnLogFactory = txnLog;
    }
    
    public FileTxnSnapLog getTxnLogFactory() {
        return this.txnLogFactory;
    }

    public String getState() {
        return "standalone";
    }

    public void dumpEphemerals(PrintWriter pwriter) {
    	zkDb.dumpEphemerals(pwriter);
    }
    
    /**
     * return the total number of client connections that are alive
     * to this server
     */
    public int getNumAliveConnections() {
        return serverCnxnFactory.getNumAliveConnections();
    }

    /**
     * 其首先将传递的ByteBuffer进行反序列化，转化为相应的ConnectRequest，之后进行一系列判断（可能抛出异常），
     * 然后获取并判断该ConnectRequest中会话id是否为0，若为0，则表示可以创建会话，否则，重新打开会话。
     */
    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        BinaryInputArchive bia = BinaryInputArchive.getArchive(new ByteBufferInputStream(incomingBuffer));
        /**
         * Zookeeper客户端和服务器在创建会话时，会发送ConnectRequest请求，该请求包含协议版本号protocolVersion、
         * 最近一次接收到服务器ZXID lastZxidSeen、会话超时时间timeOut、会话标识sessionId和会话密码passwd。
         */
        ConnectRequest connReq = new ConnectRequest();
        connReq.deserialize(bia, "connect");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session establishment request from client "
                    + cnxn.getRemoteSocketAddress()
                    + " client's lastZxid is 0x"
                    + Long.toHexString(connReq.getLastZxidSeen()));
        }
        boolean readOnly = false;
        try {
            // 是否为只读
            readOnly = bia.readBool("readOnly");
            cnxn.isOldClient = false;
        } catch (IOException e) {
            // this is ok -- just a packet from an old client which
            // doesn't contain readOnly field
            LOG.warn("Connection request from old client "
                    + cnxn.getRemoteSocketAddress()
                    + "; will be dropped if server is in r-o mode");
        }
        if (readOnly == false && this instanceof ReadOnlyZooKeeperServer) {//不为只读模式但是该服务器是只读服务器，抛出异常
            String msg = "Refusing session request for not-read-only client "
                + cnxn.getRemoteSocketAddress();
            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        /**
         * lastZxidSeen是最近一次接收到服务器ZXID，第一次连接带过来的是0
         */
        if (connReq.getLastZxidSeen() > zkDb.dataTree.lastProcessedZxid) {//请求连接的zxid大于DataTree处理的最大的zxid，抛出异常
            String msg = "Refusing session request for client "
                + cnxn.getRemoteSocketAddress()
                + " as it has seen zxid 0x"
                + Long.toHexString(connReq.getLastZxidSeen())
                + " our last zxid is 0x"
                + Long.toHexString(getZKDatabase().getDataTreeLastProcessedZxid())
                + " client must try another server";

            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        // 获取超时时间
        int sessionTimeout = connReq.getTimeOut();
        // 获取密码
        byte passwd[] = connReq.getPasswd();
        // 获取最短超时时间
        int minSessionTimeout = getMinSessionTimeout();
        if (sessionTimeout < minSessionTimeout) {
            sessionTimeout = minSessionTimeout;
        }
        // 获取最长超时时间
        int maxSessionTimeout = getMaxSessionTimeout();
        if (sessionTimeout > maxSessionTimeout) {
            sessionTimeout = maxSessionTimeout;
        }
        // 设置超时时间
        cnxn.setSessionTimeout(sessionTimeout);
        // We don't want to receive any packets until we are sure that the
        // session is setup
        // 不接收任何packet，直到会话创建成功
        cnxn.disableRecv();
        long sessionId = connReq.getSessionId();
        if (sessionId != 0) {// 表示重新创建会话  值得注意的地方是重连的调用
            long clientSessionId = connReq.getSessionId();
            LOG.info("Client attempting to renew session 0x"
                    + Long.toHexString(clientSessionId)
                    + " at " + cnxn.getRemoteSocketAddress());
            serverCnxnFactory.closeSession(sessionId);// 关闭会话
            cnxn.setSessionId(sessionId);// 设置会话id
            reopenSession(cnxn, sessionId, passwd, sessionTimeout);// 重新打开会话
        } else {
            LOG.info("Client attempting to establish new session at "
                    + cnxn.getRemoteSocketAddress());
            createSession(cnxn, passwd, sessionTimeout);// 创建会话
        }
    }

    public boolean shouldThrottle(long outStandingCount) {
        if (getGlobalOutstandingLimit() < getInProcess()) {
            return outStandingCount > 0;
        }
        return false; 
    }

    /**
     * 简单看一下ZooKeeperServer是如何处理收到的数据包的，可以看到如何把二进制的请求序列化成Java对象来使用:
        1.先用ByteBufferInputStream来将incomingBuffer封装成流，然后用Jute的接口读到RequestHeader对象，这个对象实现了Jute的Record接口
        2.RequestHeader只有两个属性，xid表示事务id，type表示请求的类型
        3.如果是auth类型的请求，从incomingBuffer中读取数据，反序列化到AuthPacket中，然后调用AuthenticationProvider来进行认证
        4.如果是sasl的请求，执行相应的代码
        5.对于其他的事务请求，构造一个Request对象，进入到submitRequest方法去执行相应的事务请求。

     可以看到ZooKeeper的请求分为了两部分，RequestHeader表示消息头，剩余部分表示消息体。消息头标示了消息的类型

     该函数首先将传递的ByteBuffer进行反序列，转化为相应的RequestHeader，然后根据该RequestHeader判断是否需要认证，
     若认证失败，则构造认证失败的响应并发送给客户端，然后关闭连接，并且再补接收任何packet。若认证成功，则构造认证成功的响应并发送给客户端。
     若不需要认证，则再判断其是否为SASL类型，若是，则进行处理，然后构造响应并发送给客户端，否则，构造请求并且提交请求。
     */
    public void processPacket(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // We have the request, now process and setup for next
        InputStream bais = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        //创建请求头
        RequestHeader h = new RequestHeader();
        //将头反序列化为RequestHeader
        h.deserialize(bia, "header");
        // Through the magic of byte buffers, txn will not be
        // pointing
        // to the start of the txn
        //slice() 方法根据现有的缓冲区创建一个 子缓冲区。也就是它创建一个新的缓冲区，新缓冲区与原来的缓冲区的一部分共享数据
        incomingBuffer = incomingBuffer.slice();
        if (h.getType() == OpCode.auth) {// 需要进行认证（有密码）
            LOG.info("got auth packet " + cnxn.getRemoteSocketAddress());
            AuthPacket authPacket = new AuthPacket();
            //将ByteBuffer转化为AuthPacket
            ByteBufferInputStream.byteBuffer2Record(incomingBuffer, authPacket);
            //获取AuthPacket的模式
            String scheme = authPacket.getScheme();
            AuthenticationProvider ap = ProviderRegistry.getProvider(scheme);
            Code authReturn = KeeperException.Code.AUTHFAILED;
            if(ap != null) {
                try {
                    //进行认证
                    authReturn = ap.handleAuthentication(cnxn, authPacket.getAuth());
                } catch(RuntimeException e) {
                    LOG.warn("Caught runtime exception from AuthenticationProvider: " + scheme + " due to " + e);
                    authReturn = KeeperException.Code.AUTHFAILED;                   
                }
            }
            if (authReturn!= KeeperException.Code.OK) { //认证失败
                if (ap == null) {
                    LOG.warn("No authentication provider for scheme: "
                            + scheme + " has "
                            + ProviderRegistry.listProviders());
                } else {
                    LOG.warn("Authentication failed for scheme: " + scheme);
                }
                // send a response...
                // 构造响应头
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
                        KeeperException.Code.AUTHFAILED.intValue());
                // 发送响应
                cnxn.sendResponse(rh, null, null);
                // ... and close connection
                // 关闭连接的信息
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
                // 不接收任何packet
                cnxn.disableRecv();
            } else {// 认证成功
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authentication succeeded for scheme: "
                              + scheme);
                }
                LOG.info("auth success " + cnxn.getRemoteSocketAddress());
                // 构造响应头
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
                        KeeperException.Code.OK.intValue());
                // 发送响应
                cnxn.sendResponse(rh, null, null);
            }
            return;
        } else {
            if (h.getType() == OpCode.sasl) { // 为SASL类型
                // 处理SASL
                Record rsp = processSasl(incomingBuffer,cnxn);
                // 构造响应头
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.OK.intValue());
                // 发送响应
                cnxn.sendResponse(rh,rsp, "response"); // not sure about 3rd arg..what is it?
                return;
            }
            else {// 不为SASL类型
                // 创建请求
                Request si = new Request(cnxn, cnxn.getSessionId(), h.getXid(),
                  h.getType(), incomingBuffer, cnxn.getAuthInfo());
                // 设置请求所有者
                si.setOwner(ServerCnxn.me);
                // 提交请求
                submitRequest(si);
            }
        }
        cnxn.incrOutstandingRequests(h);
    }

    private Record processSasl(ByteBuffer incomingBuffer, ServerCnxn cnxn) throws IOException {
        LOG.debug("Responding to client SASL token.");
        GetSASLRequest clientTokenRecord = new GetSASLRequest();
        ByteBufferInputStream.byteBuffer2Record(incomingBuffer,clientTokenRecord);
        byte[] clientToken = clientTokenRecord.getToken();
        LOG.debug("Size of client SASL token: " + clientToken.length);
        byte[] responseToken = null;
        try {
            ZooKeeperSaslServer saslServer  = cnxn.zooKeeperSaslServer;
            try {
                // note that clientToken might be empty (clientToken.length == 0):
                // if using the DIGEST-MD5 mechanism, clientToken will be empty at the beginning of the
                // SASL negotiation process.
                responseToken = saslServer.evaluateResponse(clientToken);
                if (saslServer.isComplete() == true) {
                    String authorizationID = saslServer.getAuthorizationID();
                    LOG.info("adding SASL authorization for authorizationID: " + authorizationID);
                    cnxn.addAuthInfo(new Id("sasl",authorizationID));
                }
            }
            catch (SaslException e) {
                LOG.warn("Client failed to SASL authenticate: " + e, e);
                if ((System.getProperty("zookeeper.allowSaslFailedClients") != null)
                  &&
                  (System.getProperty("zookeeper.allowSaslFailedClients").equals("true"))) {
                    LOG.warn("Maintaining client connection despite SASL authentication failure.");
                } else {
                    LOG.warn("Closing client connection due to SASL authentication failure.");
                    cnxn.close();
                }
            }
        }
        catch (NullPointerException e) {
            LOG.error("cnxn.saslServer is null: cnxn object did not initialize its saslServer properly.");
        }
        if (responseToken != null) {
            LOG.debug("Size of server SASL response: " + responseToken.length);
        }
        // wrap SASL response token to client inside a Response object.
        return new SetSASLResponse(responseToken);
    }
    
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        ProcessTxnResult rc;
        int opCode = hdr.getType();
        long sessionId = hdr.getClientId();
        rc = getZKDatabase().processTxn(hdr, txn);
        if (opCode == OpCode.createSession) {
            if (txn instanceof CreateSessionTxn) {
                CreateSessionTxn cst = (CreateSessionTxn) txn;
                sessionTracker.addSession(sessionId, cst
                        .getTimeOut());
            } else {
                LOG.warn("*****>>>>> Got "
                        + txn.getClass() + " "
                        + txn.toString());
            }
        } else if (opCode == OpCode.closeSession) {
            sessionTracker.removeSession(sessionId);
        }
        return rc;
    }

    /**
     * This method is used to register the ZooKeeperServerShutdownHandler to get
     * server's error or shutdown state change notifications.
     * {@link ZooKeeperServerShutdownHandler#handle(State)} will be called for
     * every server state changes {@link #setState(State)}.
     *
     * @param zkShutdownHandler shutdown handler
     */
    void registerServerShutdownHandler(ZooKeeperServerShutdownHandler zkShutdownHandler) {
        this.zkShutdownHandler = zkShutdownHandler;
    }
}
