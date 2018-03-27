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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 *
 * 1、NIOServerCnxn继承了ServerCnxn抽象类，是ServerCnxn的NIO实现方式，用NIO方式来处理与客户端之间的通信，单线程处理
 * 2、NIOServerCnxn维护了服务器与客户端之间的Socket通道、用于存储传输内容的缓冲区、会话ID、ZooKeeper服务器等
 * 3、ServerCnxn这个类代表了一个客户端与一个server的连接，同一个IP客户端可能会创建多个到Server的连接，就会存在多个ServerCnxn
 */
public class NIOServerCnxn extends ServerCnxn {
    static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    NIOServerCnxnFactory factory;

    final SocketChannel sock;

    protected final SelectionKey sk;//对于ACCEPT，CONNECT，READ，WRITE感兴趣的key组合

    boolean initialized;//初始化标记

    ByteBuffer lenBuffer = ByteBuffer.allocate(4);//分配四个字节缓冲区,用于读取len长度,空间大小绝对不会变

    ByteBuffer incomingBuffer = lenBuffer;//读取输入流，会根据读取到的len再分配对应的长度

    LinkedBlockingQueue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();//输入的缓冲队列

    int sessionTimeout;//会话超时时间

    protected final ZooKeeperServer zkServer;//zkServer服务器

    //已提交但是尚未回复的请求数
    int outstandingRequests;

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     */
    //回话ID
    long sessionId;
    //下个会话ID
    static long nextSessionId = 1;
    int outstandingLimit = 1;//默认的，能够容忍的已提交但是尚未回复的请求数，后面会重新赋值

    /**
     * 对Socket通道进行相应设置，如设置TCP连接无延迟、获取客户端的IP地址并将此信息进行记录
       最后设置SelectionKey感兴趣的操作类型为READ,准备读取后续消息

       说明：在构造函数中会对Socket通道进行相应设置，如设置TCP连接无延迟、获取客户端的IP地址并将此信息进行记录，
            方便后续认证，最后设置SelectionKey感兴趣的操作类型为READ。
     */
    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
            SelectionKey sk, NIOServerCnxnFactory factory) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) {
            //获取能够容忍的 接收到但是还没有回复的请求的数量
            outstandingLimit = zk.getGlobalOutstandingLimit();
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not
         * block */
        //设置linger为false，以便在socket关闭时不会阻塞
        sock.socket().setSoLinger(false, -1);
        //获取IP地址
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        //认证信息中添加IP地址
        authInfo.add(new Id("ip", addr.getHostAddress()));
        //初始对READ感兴趣(之前已经连接上了，即处理过ACCEPT了)
        sk.interestOps(SelectionKey.OP_READ);
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in 
            * a tight while loop
            */
           sock.configureBlocking(true);
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.write(bb);
               }
               packetSent();
           } 
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }
    
    public void sendBuffer(ByteBuffer bb) {
        try {
            internalSendBuffer(bb);
        } catch(Exception e) {
            LOG.error("Unexpected Exception: ", e);
        }
    }

    /**
     * This method implements the internals of sendBuffer. We
     * have separated it from send buffer to be able to catch
     * exceptions when testing.
     *
     * @param bb Buffer to send.
     *
     * 发送的核心函数
     *
     * 主要逻辑如下:
        如果不是"关闭"的ByteBuffer，如果能用NIO的方式就用NIO的方式，加入outgoingBuffers队列，否则就直接同步发送了

        说明：该函数将缓冲写入socket中，其大致处理可以分为两部分，首先会判断ByteBuffer是否为关闭连接的信号，
                并且当感兴趣的集合中没有写操作时，其会立刻将缓存写入socket
     */
    protected void internalSendBuffer(ByteBuffer bb) {
        if (bb != ServerCnxnFactory.closeConn) {//如果不是"关闭"的回复   // 不关闭连接
            // We check if write interest here because if it is NOT set,
            // nothing is queued, so we can try to send the buffer right
            // away without waking up the selector
            //首先检查interestOps中是否存在WRITE操作，如果没有，则表示直接发送缓冲而不必先唤醒selector
            if(sk.isValid() &&
                    ((sk.interestOps() & SelectionKey.OP_WRITE) == 0)) {//如果目前selectionKey还未注册WRITE(代表不能用NIO的方式)，则直接写
                try {
                    // 将缓冲写入socket
                    sock.write(bb);
                } catch (IOException e) {
                    // we are just doing best effort right now
                }
            }
            // if there is nothing left to send, we are done
            if (bb.remaining() == 0) {//bb中的内容已经被全部读取，则更新发包次数
                //统计发送包信息（调用ServerCnxn方法）
                packetSent();
                return;
            }
        }

        //当缓冲区被正常的写入到socket后，会直接返回，然而，当原本就对写操作感兴趣时，其会走如下流程,
        //首先会唤醒上个被阻塞的selection操作，然后将缓冲添加至outgoingBuffers队列中，后续再进行发送。
        synchronized(this.factory){ //同步块
            //让第一个还没返回（阻塞）的selection操作马上返回结果
            sk.selector().wakeup();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                        + " is valid: " + sk.isValid());
            }
            outgoingBuffers.add(bb);//添加到发送缓存队列
            if (sk.isValid()) {// key是否合法
                sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);//注册WRITE事件
            }
        }
    }

    /** Read the request payload (everything following the length prefix) */
    //读取payload请求，即非cmd的请求
    private void readPayload() throws IOException, InterruptedException {
        if (incomingBuffer.remaining() != 0) { // have we read length bytes?
            int rc = sock.read(incomingBuffer); // sock is non-blocking, so ok
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from client sessionid 0x"
                        + Long.toHexString(sessionId)
                        + ", likely client has closed socket");
            }
        }

        if (incomingBuffer.remaining() == 0) { // have we read length bytes?
            packetReceived();//更新统计数据
            incomingBuffer.flip();
            if (!initialized) {
                readConnectRequest();//读取连接请求
            } else {
                readRequest();//读取请求
            }
            lenBuffer.clear();
            incomingBuffer = lenBuffer;//还原成只读4个字节的byte
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    @Override
    public InetAddress getSocketAddress() {
        if (sock == null) {
            return null;
        }

        return sock.socket().getInetAddress();
    }

    /**
     * Handles read/write IO on connection.
     * 1.可读，那么根据最开始4个字节的int，判断是否是payload，如果不是的话，代表是cmd，调用对应的处理函数写进printWriter
          如果是的话，分配对应len的空间给buffer，读取后续请求内容
     2.如果可写，那么遍历outgoingBuffers,然后每次写满一个64k的buffer空间（可能有分片的操作），然后进行发送


     NIOServerCnxn的核心方法是doIO, 它实现了SelectionKey被Selector选出后，SocketChannel如何进行读写

     SocketChannel从客户端读数据的过程：
        1.NIOServerCnxc维护了两个读数据的ByteBuffer, 一个是lenBuffer=ByteBuffer.allocate(4)，4个字节的ByteBuffer，
            另一个是ByteBuffer incomingBuffer表示用来存放读数据ByteBuffer, 初始状态下incomingBuffer指向lenBuffer
        2、最开始读取4字节数据到incomingBuffer中，因为lenBuffer和incomingBuffer都指向同一个ByteBuffer，它们内容一致
        3、然后根据读取4字节转成int，然后判断是否是4字节命令，如果是4字节命令，就调用对应的线程CommandThread，启动单独的线程去执行对应的命令；否则则将读取的int代表后面数据的length，
            执行incomingBuffer = ByteBuffer.allocate(len)，进入到readPayload分支。在readPayload判断incomingBuffer是否满包，如果不是，就尝试读一次SocketChanel。
            如果这时候满包了，就调用flip方法切换到读模式，如果是第一次读到请求，就进入readConnectRequest，如果不是就进入到readRequest。 最后 incomingBuffer = lenBuffer; 再次指向lenBuffer，读下一个请求。

     NIOServerCnxn写数据的过程如下：
        1.创建一个LinkedBlockingQueue<ByteBuffer>类型的outgoingBuffers来优化写，可以一次写多个ByteBuffer
        2.如果SelectionKey是因为写消息被Selector选中的，先判断outgoingBuffers的长度是否大于0，如果大于0，就把outgoingBuffers中的ByteBuffer的数据都复制到factory.directBuffer这个直接内存的缓冲区中，
            如果directBuffer满了或者outgoingBuffers都已经复制到directBuffer了，就调用它的flip方法把它切换到读模式，然后把它的数据写入到SocketChannel中去。
            由此可见，每次写的时候，都是从directBuffer写到SocketChannel中去的，利用直接内存优化了写操作。写完后清理一下outgoingBuffers，把已经写完的ByteBuffer清理掉
        3.如果outgoingBuffers都写完了，就把SocketChannel切换到读模式中，关闭对写标志位的监听。如果没写完，继续监听写请求。

     NIOServerCnxn写操作的入口方法有两个，一个是同步IO的sendBufferSync， 一个是NIO的sendBuffer。
     1.基于同步IO的sendBufferSync方法直接把SocketChannel设置为阻塞模式，然后直接写到Socket中去。
        上面提到的相应4字符命令的场景，就是使用了sendBufferSync的方法，直接写。
     2. sendBuffer方法使用了NIO，它主要是因为使用了outgoingBuffers队列来优化写操作，可以一次写多个ByteBuffer。
        写的时候，先加入到outgoingBuffers，然后设置SelectionKey的写标志位，这样在下次Selector执行select方法时，可以进行写的动作

     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x"
                         + Long.toHexString(sessionId));

                return;
            }
            if (k.isReadable()) {
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException(
                            "Unable to read additional data from client sessionid 0x"
                            + Long.toHexString(sessionId)
                            + ", likely client has closed socket");
                }
                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    if (incomingBuffer == lenBuffer) { // start of next request
                        incomingBuffer.flip();
                        isPayload = readLength(k);//读取len，判断是否是payload
                        incomingBuffer.clear();
                    } else {
                        // continuation
                        isPayload = true;
                    }
                    if (isPayload) { // not the case for 4letterword
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            if (k.isWritable()) {//如果可写
                // ZooLog.logTraceMessage(LOG,
                // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK
                // "outgoingBuffers.size() = " +
                // outgoingBuffers.size());
                if (outgoingBuffers.size() > 0) {//发送队列不为空
                    // ZooLog.logTraceMessage(LOG,
                    // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK,
                    // "sk " + k + " is valid: " +
                    // k.isValid());

                    /*
                     * This is going to reset the buffer position to 0 and the
                     * limit to the size of the buffer, so that we can fill it
                     * with data from the non-direct buffers that we need to
                     * send.
                     */
                    ByteBuffer directBuffer = factory.directBuffer;
                    directBuffer.clear();

                    for (ByteBuffer b : outgoingBuffers) {
                        if (directBuffer.remaining() < b.remaining()) {//如果分配空间剩余不够，就进行分片
                            /*
                             * When we call put later, if the directBuffer is to
                             * small to hold everything, nothing will be copied,
                             * so we've got to slice the buffer if it's too big.
                             */
                            b = (ByteBuffer) b.slice().limit(
                                    directBuffer.remaining());
                        }
                        /*
                         * put() is going to modify the positions of both
                         * buffers, put we don't want to change the position of
                         * the source buffers (we'll do that after the send, if
                         * needed), so we save and reset the position after the
                         * copy
                         */
                        int p = b.position();
                        directBuffer.put(b);
                        b.position(p);
                        if (directBuffer.remaining() == 0) {//写满一个buffer
                            break;
                        }
                    }
                    /*
                     * Do the flip: limit becomes position, position gets set to
                     * 0. This sets us up for the write.
                     */
                    directBuffer.flip();

                    int sent = sock.write(directBuffer);//写入socket
                    ByteBuffer bb;

                    // Remove the buffers that we have sent
                    while (outgoingBuffers.size() > 0) {
                        bb = outgoingBuffers.peek();
                        if (bb == ServerCnxnFactory.closeConn) {
                            throw new CloseRequestException("close requested");
                        }
                        int left = bb.remaining() - sent;
                        if (left > 0) {//如果还有内容没有发送
                            /*
                             * We only partially sent this buffer, so we update
                             * the position and exit the loop.
                             */
                            bb.position(bb.position() + sent);
                            break;
                        }
                        packetSent();//更新统计数据
                        /* We've sent the whole buffer, so drop the buffer */
                        sent -= bb.remaining();
                        outgoingBuffers.remove();
                    }
                    // ZooLog.logTraceMessage(LOG,
                    // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK, "after send,
                    // outgoingBuffers.size() = " + outgoingBuffers.size());
                }

                synchronized(this.factory){
                    if (outgoingBuffers.size() == 0) {
                        if (!initialized
                                && (sk.interestOps() & SelectionKey.OP_READ) == 0) {
                            throw new CloseRequestException("responded to info probe");
                        }
                        sk.interestOps(sk.interestOps()
                                & (~SelectionKey.OP_WRITE));
                    } else {
                        sk.interestOps(sk.interestOps()
                                | SelectionKey.OP_WRITE);
                    }
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x"
                     + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("EndOfStreamException stack trace", e);
            }
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x"
                     + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }
    //读取非连接的请求
    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }
    //增加尚未处理的请求个数
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            synchronized (this) {
                outstandingRequests++;
            }
            synchronized (this.factory) {        
                // check throttling
                if (zkServer.getInProcess() > outstandingLimit) {//如果超过阈值了，就禁止读
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Throttling recv " + zkServer.getInProcess());
                    }
                    disableRecv();
                    // following lines should not be needed since we are
                    // already reading
                    // } else {
                    // enableRecv();
                }
            }
        }

    }

    public void disableRecv() {
        sk.interestOps(sk.interestOps() & (~SelectionKey.OP_READ));
    }

    public void enableRecv() {
        synchronized (this.factory) {
            sk.selector().wakeup();
            if (sk.isValid()) {
                int interest = sk.interestOps();
                if ((interest & SelectionKey.OP_READ) == 0) {
                    sk.interestOps(interest | SelectionKey.OP_READ);
                }
            }
        }
    }
    //读取连接请求,调用ZooKeeperServer相关逻辑
    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;//标记初始化完成
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     * 
     * @param pwriter
     *            the pwriter for a command socket
     * cleanupWriterSocket函数完成对应cmd处理器写完printWriter之后进行关闭
     */
    private void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     *
     * 1、SendBufferWriter定义一些Writer的实现，来完成cmd处理时的一些输出
     * 2、该类用来将给客户端的响应进行分块避免response太大，没有写完而一直占用空间,因此对response分块
     */
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();
        
        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            //当强制发送并且sb大小大于0，或者sb大小大于2048即发送缓存
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            checkFlush(true);// 关闭之前需要强制性发送缓存
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);//sb写入内容
            checkFlush(false);//非强制发送
        }
    }

    private static final String ZK_NOT_SERVING =
        "This ZooKeeper instance is not currently serving requests";
    
    /**
     * Set of threads for commmand ports. All the 4
     * letter commands are run via a thread. Each class
     * maps to a corresponding 4 letter command. CommandThread
     * is the abstract class from which all the others inherit.
     *
     * 1、CommandThread完成不同cmd的处理
     * 2、用于处理ServerCnxn中的定义的命令，如"ruok","stmk".
     *      其主要逻辑定义在commandRun方法中，在子类中各自实现，每个命令使用单独的线程进行处理
     *
     * 3、该类用于处理ServerCnxn中的定义的命令，其主要逻辑定义在commandRun方法中，在子类中各自实现，
     *      这是一种典型的工厂方法，每个子类对应着一个命令，每个命令使用单独的线程进行处理
     */
    private abstract class CommandThread extends Thread {
        PrintWriter pw;
        
        CommandThread(PrintWriter pw) {
            this.pw = pw;
        }
        
        public void run() {
            try {
                commandRun();
            } catch (IOException ie) {
                LOG.error("Error in running command ", ie);
            } finally {
                cleanupWriterSocket(pw);
            }
        }
        
        public abstract void commandRun() throws IOException;
    }
    
    private class RuokCommand extends CommandThread {
        public RuokCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            pw.print("imok");
            
        }
    }
    
    private class TraceMaskCommand extends CommandThread {
        TraceMaskCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            long traceMask = ZooTrace.getTextTraceLevel();
            pw.print(traceMask);
        }
    }
    
    private class SetTraceMaskCommand extends CommandThread {
        long trace = 0;
        SetTraceMaskCommand(PrintWriter pw, long trace) {
            super(pw);
            this.trace = trace;
        }
        
        @Override
        public void commandRun() {
            pw.print(trace);
        }
    }

    //一般步骤：继承CommandThread，实现commandRun方法，完成对应操作，写入PrintWriter就行
    private class EnvCommand extends CommandThread {
        EnvCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            List<Environment.Entry> env = Environment.list();

            pw.println("Environment:");
            for(Environment.Entry e : env) {
                pw.print(e.getKey());
                pw.print("=");
                pw.println(e.getValue());
            }
            
        } 
    }
    
    private class ConfCommand extends CommandThread {
        ConfCommand(PrintWriter pw) {
            super(pw);
        }
            
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                zkServer.dumpConf(pw);
            }
        }
    }
    
    private class StatResetCommand extends CommandThread {
        public StatResetCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else { 
                zkServer.serverStats().reset();
                pw.println("Server stats reset.");
            }
        }
    }
    
    private class CnxnStatResetCommand extends CommandThread {
        public CnxnStatResetCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                synchronized(factory.cnxns){
                    for(ServerCnxn c : factory.cnxns){
                        c.resetStats();
                    }
                }
                pw.println("Connection stats reset.");
            }
        }
    }

    private class DumpCommand extends CommandThread {
        public DumpCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else {
                pw.println("SessionTracker dump:");
                zkServer.sessionTracker.dumpSessions(pw);
                pw.println("ephemeral nodes dump:");
                zkServer.dumpEphemerals(pw);
            }
        }
    }
    
    private class StatCommand extends CommandThread {
        int len;
        public StatCommand(PrintWriter pw, int len) {
            super(pw);
            this.len = len;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else {   
                pw.print("Zookeeper version: ");
                pw.println(Version.getFullVersion());
                if (zkServer instanceof ReadOnlyZooKeeperServer) {
                    pw.println("READ-ONLY mode; serving only " +
                               "read-only clients");
                }
                if (len == statCmd) {
                    LOG.info("Stat command output");
                    pw.println("Clients:");
                    // clone should be faster than iteration
                    // ie give up the cnxns lock faster
                    HashSet<NIOServerCnxn> cnxnset;
                    synchronized(factory.cnxns){
                        cnxnset = (HashSet<NIOServerCnxn>)factory
                        .cnxns.clone();
                    }
                    for(NIOServerCnxn c : cnxnset){
                        c.dumpConnectionInfo(pw, true);
                        pw.println();
                    }
                    pw.println();
                }
                pw.print(zkServer.serverStats().toString());
                pw.print("Node count: ");
                pw.println(zkServer.getZKDatabase().getNodeCount());
            }
            
        }
    }
    
    private class ConsCommand extends CommandThread {
        public ConsCommand(PrintWriter pw) {
            super(pw);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                // clone should be faster than iteration
                // ie give up the cnxns lock faster
                HashSet<NIOServerCnxn> cnxns;
                synchronized (factory.cnxns) {
                    cnxns = (HashSet<NIOServerCnxn>) factory.cnxns.clone();
                }
                for (NIOServerCnxn c : cnxns) {
                    c.dumpConnectionInfo(pw, false);
                    pw.println();
                }
                pw.println();
            }
        }
    }
    
    private class WatchCommand extends CommandThread {
        int len = 0;
        public WatchCommand(PrintWriter pw, int len) {
            super(pw);
            this.len = len;
        }

        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                DataTree dt = zkServer.getZKDatabase().getDataTree();
                if (len == wchsCmd) {
                    dt.dumpWatchesSummary(pw);
                } else if (len == wchpCmd) {
                    dt.dumpWatches(pw, true);
                } else {
                    dt.dumpWatches(pw, false);
                }
                pw.println();
            }
        }
    }

    private class MonitorCommand extends CommandThread {

        MonitorCommand(PrintWriter pw) {
            super(pw);
        }

        @Override
        public void commandRun() {
            if(!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
                return;
            }
            ZKDatabase zkdb = zkServer.getZKDatabase();
            ServerStats stats = zkServer.serverStats();

            print("version", Version.getFullVersion());

            print("avg_latency", stats.getAvgLatency());
            print("max_latency", stats.getMaxLatency());
            print("min_latency", stats.getMinLatency());

            print("packets_received", stats.getPacketsReceived());
            print("packets_sent", stats.getPacketsSent());
            print("num_alive_connections", stats.getNumAliveClientConnections());

            print("outstanding_requests", stats.getOutstandingRequests());

            print("server_state", stats.getServerState());
            print("znode_count", zkdb.getNodeCount());

            print("watch_count", zkdb.getDataTree().getWatchCount());
            print("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
            print("approximate_data_size", zkdb.getDataTree().approximateDataSize());

            OSMXBean osMbean = new OSMXBean();
            if (osMbean != null && osMbean.getUnix() == true) {
                print("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
                print("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());
            }

            if(stats.getServerState().equals("leader")) {
                Leader leader = ((LeaderZooKeeperServer)zkServer).getLeader();

                print("followers", leader.getLearners().size());
                print("synced_followers", leader.getForwardingFollowers().size());
                print("pending_syncs", leader.getNumPendingSyncs());
            }
        }

        private void print(String key, long number) {
            print(key, "" + number);
        }

        private void print(String key, String value) {
            pw.print("zk_");
            pw.print(key);
            pw.print("\t");
            pw.println(value);
        }

    }

    private class IsroCommand extends CommandThread {

        public IsroCommand(PrintWriter pw) {
            super(pw);
        }

        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.print("null");
            } else if (zkServer instanceof ReadOnlyZooKeeperServer) {
                pw.print("ro");
            } else {
                pw.print("rw");
            }
        }
    }

    private class NopCommand extends CommandThread {
        private String msg;

        public NopCommand(PrintWriter pw, String msg) {
            super(pw);
            this.msg = msg;
        }

        @Override
        public void commandRun() {
            pw.println(msg);
        }
    }

    /** Return if four letter word found and responded to, otw false **/
    /**
     * 验证int值是否对应特定的cmd，是的话写入对应回复到PrintWriter
     * ZooKeeper支持某些特定的四字命令(The Four Letter Words)与其进行交互。它们大多是查询命令，用来获取 ZooKeeper 服务的当前状态及相关信息。用户在客户端可以通过 telnet 或 nc 向 ZooKeeper 提交相应的命令。
     */
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
    throws IOException
    {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!ServerCnxn.isKnown(len)) {
            return false;
        }

        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open. 
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        String cmd = ServerCnxn.getCommandString(len);
        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!ServerCnxn.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, cmd + " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == ruokCmd) {
            RuokCommand ruok = new RuokCommand(pwriter);
            ruok.start();
            return true;
        } else if (len == getTraceMaskCmd) {
            TraceMaskCommand tmask = new TraceMaskCommand(pwriter);
            tmask.start();
            return true;
        } else if (len == setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }

            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, traceMask);
            setMask.start();
            return true;
        } else if (len == enviCmd) {
            EnvCommand env = new EnvCommand(pwriter);
            env.start();
            return true;
        } else if (len == confCmd) {
            ConfCommand ccmd = new ConfCommand(pwriter);
            ccmd.start();
            return true;
        } else if (len == srstCmd) {
            StatResetCommand strst = new StatResetCommand(pwriter);
            strst.start();
            return true;
        } else if (len == crstCmd) {
            CnxnStatResetCommand crst = new CnxnStatResetCommand(pwriter);
            crst.start();
            return true;
        } else if (len == dumpCmd) {
            DumpCommand dump = new DumpCommand(pwriter);
            dump.start();
            return true;
        } else if (len == statCmd || len == srvrCmd) {
            StatCommand stat = new StatCommand(pwriter, len);
            stat.start();
            return true;
        } else if (len == consCmd) {
            ConsCommand cons = new ConsCommand(pwriter);
            cons.start();
            return true;
        } else if (len == wchpCmd || len == wchcCmd || len == wchsCmd) {
            WatchCommand wcmd = new WatchCommand(pwriter, len);
            wcmd.start();
            return true;
        } else if (len == mntrCmd) {
            MonitorCommand mntr = new MonitorCommand(pwriter);
            mntr.start();
            return true;
        } else if (len == isroCmd) {
            IsroCommand isro = new IsroCommand(pwriter);
            isro.start();
            return true;
        }
        return false;
    }

    /** Reads the first 4 bytes of lenBuffer, which could be true length or
     *  four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     *
     * 读取前四个字节，判断是否是payload这种请求：
     *  payload:翻译是"有效载荷"，代表的是前面四个字节的int代表len，后续有对应len长度的buffer
        非payload:前面四个字节int，对应ServerCnxn中定义的不同cmd，如“conf”(对应int 1668247142)，
            “cons”(对应 1668247155)等，不同的cmd后续可能跟着不同的，特定长度的buffer

        读取前4个字节代表int，如果还没有初始化，并且int值是特定cmd对应的int，那么就当成是cmd，否则给incomingBuffer分配对应len的空间
     */
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {//如果没有初始化，并且是cmd的话，就写对应的printWriter回复
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);//分配对应len的空间
        return true;
    }

    public long getOutstandingRequests() {
        synchronized (this) {
            synchronized (this.factory) {
                return outstandingRequests;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public String toString() {
        return "NIOServerCnxn object with sock = " + sock + " and sk = " + sk;
    }

    /*
     * Close the cnxn and remove it from the factory cnxns list.
     * 
     * This function returns immediately if the cnxn is not on the cnxns list.
     */
    @Override
    public void close() {
        factory.removeCnxn(this);

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        closeSock();

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }
    }

    /**
     * Close resources associated with the sock of this cnxn. 
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.info("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                        " which had sessionid 0x" + Long.toHexString(sessionId) :
                        " (no session established for client)"));
        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
            // XXX The next line doesn't seem to be needed, but some posts
            // to forums suggest that it is needed. Keep in mind if errors in
            // this section arise.
            // factory.selector.wakeup();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }
    
    private final static byte fourBytes[] = new byte[4];

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#sendResponse(org.apache.zookeeper.proto.ReplyHeader,
     *      org.apache.jute.Record, java.lang.String)
     *
     *  主要是进行一些序列化的操作，然后把对应长度len写入，方便client读,完成一些数据统计，更新的操作
     */
    @Override
    synchronized public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Make space for length
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            try {
                baos.write(fourBytes);
                bos.writeRecord(h, "header");
                if (r != null) {
                    bos.writeRecord(r, tag);
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("Error serializing response");
            }
            byte b[] = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.putInt(b.length - 4).rewind();//把len-4放入byteBuffer，代表后续内容的长度
            sendBuffer(bb);
            if (h.getXid() > 0) {
                synchronized(this){
                    outstandingRequests--;
                }
                // check throttling
                synchronized (this.factory) {        
                    if (zkServer.getInProcess() < outstandingLimit
                            || outstandingRequests < 1) {
                        sk.selector().wakeup();
                        enableRecv();//开启写
                    }
                }
            }
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     *
     */
    @Override
    synchronized public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);//xid为-1表示为通知
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();//包装为WatcherEvent来提供网络传输

        sendResponse(h, e, "notification");//给client发送请求,通知WatchedEvent的发生
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        this.factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public int getInterestOps() {
        return sk.isValid() ? sk.interestOps() : 0;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (!isZKServerRunning()) {
            return null;
        }
        return zkServer.serverStats();
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }
}
