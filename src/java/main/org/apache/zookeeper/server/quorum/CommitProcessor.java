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

import java.util.ArrayList;
import java.util.LinkedList;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 *
 * 为了保证顺序的执行，CommitRequestProcessor会在接收到写请求时挂起，暂停处理其他请求。这意味着在写请求之后，接收到的任意的读请求会被阻塞，直到写请求完毕为止。通过等待来保证按照接收到的顺序来执行请求。
 *
 * 这个processor主要负责将已经完成本机submit的request和已经在集群中达成commit的request匹配，并将匹配后的request交给nextProcessor（即ToBeAppliedRequestProcessor）处理

 这是个异步处理的processor，它有两个入口：
 一个是前面的processor调用的processRequest，它把request放到队列queuedRequests中
 另一个是leader在收到follower的ack包后，调用的processAck函数，如果leader收到了足够的ack包，他会调用这个processord的commit函数，这个函数把请求放入到队列committedRequests中

 这个processor还会启动一个线程，他的主要是逻辑就是匹配queuedRequests和committedRequests中的request，匹配是基于两个队列的对首比较的，然后比较两个队列对首的以下元素：
 request的txnHeader
 request的record
 request的zxid

 如果都匹配上了就把这个request发给ToBeAppliedRequestProcessor处理


 CommitProcessor:事务提交处理器，1、对于非事务请求，该处理器会直接将其交付给下一级处理器处理；
 2、对于事务请求，其会等待集群内针对Proposal的投票直到该Proposal可被提交，利用CommitProcessor，每个服务器都可以很好地控制对事务请求的顺序处理。


 *
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /**
     * Requests that we are holding until the commit comes in.
     */
    //请求队列
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;
    //待处理的队列
    ArrayList<Request> toProcess = new ArrayList<Request>();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be true if the CommitProcessor is in a Leader pipeline.
     */
    /**
     * matchSyncs 在leader端是false，learner端是true，因为learner端sync请求需要等待leader回复，而leader端本身则不需要
     * 看sync的请求是等待leader回复，还是说直接处理，像读请求一样。对于leader是false，对于learner是true
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
        boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    volatile boolean finished = false;

    /**
     * 完全可以按照 1,4,2,3的顺序来读，

     1部分:遍历toProcess队列(非事务请求或者已经提交的事务请求),交给下一个处理器处理，清空

     4部分:只要不存在pend住的事务请求并且请求队列不为空，一直遍历请求队列直到出现第一个事务请求或者队列遍历完，其间所有非事务请求全部加入toProcess队列,代表可以直接交给下一个处理器处理的

     2部分:在请求队列remove干净或者找到了事务请求的情况下，
     如果没有提交的请求，就等待。
     如果有提交的请求，取出来，看和之前记录的下一个pend的请求是否match。
     match的话，进入toProcess队列，nextPending置空
     不match的话,(基本上是nextPending为null，不会出现不为null且不匹配的情况),进入toProcess处理

     3部分:如果 nextPending非空，就不用再去遍历请求队列，找到下一个事务请求(即4部分)，因此continue掉
     */
    @Override
    public void run() {
        try {
            //下一个未处理的事务请求(不含leader端的sync请求),只要为null，都会while循环从queuedRequests里面找到第一个事务请求，或者直到队列为空
            Request nextPending = null;
            while (!finished) {//只要没有shutdown

                /**
                 * 第一部分：遍历toProcess队列(非事务请求或者已经提交的事务请求),交给下一个处理器处理，清空
                 */
                int len = toProcess.size();
                LOG.info("len:"+len);
                for (int i = 0; i < len; i++) {//待处理队列交给下个处理器,按顺序处理
                    nextProcessor.processRequest(toProcess.get(i));
                }
                toProcess.clear();//队列清空

                /**
                 * 第二部分:在请求队列remove干净或者找到了事务请求的情况下，如果没有提交的请求，就等待。
                 如果有提交的请求，取出来，看和之前记录的下一个pend的请求是否match。
                 match的话，进入toProcess队列，nextPending置空
                 不match的话,(基本上是nextPending为null，不会出现不为null且不匹配的情况),进入toProcess处理
                 */
                synchronized (this) {//注意这里上锁，不会出现执行到过程中，queuedRequests的size变了
                    if ((queuedRequests.size() == 0 || nextPending != null)/*这部分结合尾部的while来读，要么请求队列remove干净，
                            要么从中找到一个事务请求，赋值给nextPending, 不允许size>0且nextPending == null的情况*/
                        && committedRequests.size() == 0)/*且没有已提交事务*/ {

                        wait();
                        continue;
                    }
                    // First check and see if the commit came in for the pending
                    // request
                    if ((queuedRequests.size() == 0 || nextPending != null)
                        // 不允许size>0且nextPending==null的情况
                        && committedRequests.size() > 0) {//如果有已提交的请求
                        LOG.info("22222222222222");
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        if (nextPending != null
                            && nextPending.sessionId == r.sessionId
                            && nextPending.cxid == r.cxid) {//如果和nextPending匹配
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);//加入待处理队列
                            nextPending = null;//下一个pend的请求清空
                        } else {
                            // this request came from someone else so just
                            // send the commit packet
                            //这种情况是nextPending还没有来的及设置，nextPending==null的情况(代码应该再细分一下if else),不可能出现nextPending!=null而走到了这里的情况(算异常)
                            toProcess.add(r);
                        }
                    }
                }




                // We haven't matched the pending requests, so go back to
                // waiting
                /**
                 * 第三部分：如果nextPending非空，就不用再去遍历请求队列，找到下一个事务请求(即4部分)，因此continue掉
                 */
                if (nextPending != null) {//如果还有未处理的事务请求(不含leader端的sync请求),就continue
                    continue;
                }

                /**
                 * 第四部分:只要不存在pend住的事务请求并且请求队列不为空，一直遍历请求队列直到出现第一个事务请求或者队列遍历完，其间所有非事务请求全部加入toProcess队列,代表可以直接交给下一个处理器处理的
                 */
                synchronized (this) {//这一段的目的是找到一个 给nextPending赋值
                    // Process the next requests in the queuedRequests
                    while (nextPending == null && queuedRequests.size() > 0) {
                        //只要queuedRequests队列不空，从中找到第一个 事务请求(不含leader端的sync请求),前面的其他请求全部加入待处理队列
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                            case OpCode.create:
                            case OpCode.delete:
                            case OpCode.setData:
                            case OpCode.multi:
                            case OpCode.setACL:
                            case OpCode.createSession:
                            case OpCode.closeSession:
                                //大部分事务请求直接赋给nextPending，然后break
                                nextPending = request;
                                break;
                            case OpCode.sync:
                                if (matchSyncs) {//如果需要等leader返回,该值learner端为true
                                    nextPending = request;
                                } else {
                                    toProcess.add(request);//不需要的话，直接加入待处理队列里
                                }
                                break;//leader端matchSyncs是false，learner端才需要等leader回复，这里也break
                            default:
                                toProcess.add(request);//非事务请求，都直接加入待处理队列
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    //事务请求提交
    synchronized public void commit(Request request) {
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                    new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            committedRequests.add(request);//进入已提交队列
            notifyAll();
        }
    }

    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        if (!finished) {
            queuedRequests.add(request);
            notifyAll();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
