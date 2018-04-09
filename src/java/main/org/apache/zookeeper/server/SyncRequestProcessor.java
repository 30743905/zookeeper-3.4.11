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

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 *
 * 这个处理器用来将请求记录到txLog文件中，通过批量刷盘的方式来提升io的性能，这里请求只有被写入到本地磁盘后，才会被传递到下一个处理器
 *
 * 如果是事务请求，会经过SyncRequestProcessor，SyncRequestProcessor:事务日志记录处理器。用来将事务请求记录到事务日志文件中，同时会触发Zookeeper进行数据快照。
 *
 * SyncRequestProcessor:他就是事务日志记录处理器。用来将事务请求记录到事务日志文件中，同时会触发Zookeeper进行数据快照。
 * 里面调用了很多相关方法，如rollLog,append,commit，takeSnapshot等方法，底层都是FileTxnLog，FileSnap来执行的
 *
 * 在分析了PrepRequestProcessor处理器后，接着来分析SyncRequestProcessor，该处理器将请求存入磁盘，其将请求批量的存入磁盘以提高效率，请求在写入磁盘之前是不会被转发到下个处理器的。
 *
 * 与PrepRequestProcessor一样，SyncRequestProcessor也继承了Thread类并实现了RequestProcessor接口，表示其可以作为线程使用
 *
 *
 *
 * 其中，SyncRequestProcessor维护了ZooKeeperServer实例，其用于获取ZooKeeper的数据库和其他信息；维护了一个处理请求的队列，其用于存放请求；
 * 维护了一个处理快照的线程，用于处理快照；维护了一个running标识，标识SyncRequestProcessor是否在运行；同时还维护了一个等待被刷新到磁盘的请求队列。
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    // 快照处理线程
    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    //等待被刷到磁盘的请求队列
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random(System.nanoTime());
    /**
     * The number of log entries to log before starting a snapshot
     */
    //快照的个数
    private static int snapCount = ZooKeeperServer.getSnapCount();
    
    /**
     * The number of log entries before rolling the log, number
     * is chosen randomly
     */
    // 一个随机数，用来帮助判断何时让事务日志从当前“滚”到下一个
    private static int randRoll;
    // 结束请求标识
    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }
    
    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
        randRoll = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }
    
    /**
     * Sets the value of randRoll. This method 
     * is here to avoid a findbugs warning for
     * setting a static variable in an instance
     * method. 
     * 
     * @param roll
     */
    private static void setRandRoll(int roll) {
        randRoll = roll;
    }

    /**
     * 核心方法，消费请求队列,批处理进行快照以及刷到事务日志
     */
    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            //randRoll是一个 snapCount/2以内的随机数, 避免所有机器同时进行snapshot
            // 确保所有的服务器在同一时间不是使用的同一个快照
            setRandRoll(r.nextInt(snapCount/2));
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {//没有要刷到磁盘的请求
                    si = queuedRequests.take();//消费请求队列
                } else {//有需要刷到磁盘的请求
                    si = queuedRequests.poll();
                    if (si == null) {//如果请求队列的当前请求为空
                        flush(toFlush);//刷到磁盘
                        continue;
                    }
                }
                if (si == requestOfDeath) {//结束标识请求
                    break;
                }
                if (si != null) {//请求队列取出了请求
                    // track the number of records written to the log
                    if (zks.getZKDatabase().append(si)) {//请求添加至日志文件，只有事务性请求才会返回true
                        logCount++;
                        /**
                         * randRoll的意义，可以看到语句 if (logCount > (snapCount / 2 + randRoll))
                         * 这是用来判断logCount是否足够，如果足够了，代表一个事务日志记录的量够了，
                         * 下面调用rollLog，就会生成下一个事务日志文件了。
                         */
                        if (logCount > (snapCount / 2 + randRoll)) {//如果logCount到了一定的量  // 满足roll the log的条件
                            setRandRoll(r.nextInt(snapCount/2));//下一次的随机数重新选
                            // roll the log
                            zks.getZKDatabase().rollLog();//事务日志滚动到另外一个文件记录
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {//正在进行快照
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();//进行快照,将sessions和datatree保存至snapshot文件
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();//启动线程
                            }
                            logCount = 0;//重置
                        }
                    } else if (toFlush.isEmpty()) {//刷到磁盘的队列为空  // 查看此时toFlush是否为空，如果为空，说明近段时间读多写少，直接响应
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);//下个处理器处理
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();//下个处理器可以刷，就刷
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);//刷的队列添加记录
                    if (toFlush.size() > 1000) {//超过了1000条就一起刷到磁盘
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }


    /**
     *
     * @param toFlush
     * @throws IOException
     * @throws RequestProcessorException
     * flush：批处理的思想，把事务日志刷到磁盘，让下一个处理器处理
     */
    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())//队列为空，没有需要刷的
            return;

        zks.getZKDatabase().commit();//事务日志刷到磁盘
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);//下一个处理器处理
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();//下个处理器也可以刷，就刷
        }
    }

    /**
     * 队列添加requestOfDeath请求，线程结束后，调用flush函数，最后关闭nextProcessor
     */
    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
