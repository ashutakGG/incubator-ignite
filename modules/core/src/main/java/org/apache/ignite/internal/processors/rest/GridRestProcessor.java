/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.rest.client.message.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.handlers.cache.*;
import org.apache.ignite.internal.processors.rest.handlers.datastructures.*;
import org.apache.ignite.internal.processors.rest.handlers.query.*;
import org.apache.ignite.internal.processors.rest.handlers.task.*;
import org.apache.ignite.internal.processors.rest.handlers.top.*;
import org.apache.ignite.internal.processors.rest.handlers.version.*;
import org.apache.ignite.internal.processors.rest.protocols.tcp.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.processors.security.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.internal.visor.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.plugin.security.SecurityException;
import org.jsr166.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.*;
import static org.apache.ignite.plugin.security.SecuritySubjectType.*;

/**
 * Rest processor implementation.
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** HTTP protocol class name. */
    private static final String HTTP_PROTO_CLS =
        "org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol";

    /** Delay between sessions timeout checks*/
    private static final int SES_TIMEOUT_CHECK_DELAY = 1_000;

    /** Default session timout */
    private static final int DEFAULT_SES_TIMEOUT = 30_000;

    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<>();

    /** Command handlers. */
    protected final Map<GridRestCommand, GridRestCommandHandler> handlers = new EnumMap<>(GridRestCommand.class);

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Workers count. */
    private final LongAdder8 workersCnt = new LongAdder8();

    /** SecurityContext map. */
    private final ConcurrentMap<UUID, UUID> clientId2SesId = new ConcurrentHashMap<>();

    /** SecurityContext map. */
    private final ConcurrentMap<UUID, Session> sesId2Ses = new ConcurrentHashMap<>();

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws IgniteCheckedException {
            return handleAsync(req).get();
        }

        @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
            return handleAsync0(req);
        }
    };

    /** Sesion timeout size */
    private final long sesTtl;

    /**
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridRestResponse> handleAsync0(final GridRestRequest req) {
        if (!busyLock.tryReadLock())
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to handle request (received request while stopping grid)."));

        try {
            final GridWorkerFuture<GridRestResponse> fut = new GridWorkerFuture<>();

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "rest-proc-worker", log) {
                @Override protected void body() {
                    try {
                        IgniteInternalFuture<GridRestResponse> res = handleRequest(req);

                        res.listen(new IgniteInClosure<IgniteInternalFuture<GridRestResponse>>() {
                            @Override public void apply(IgniteInternalFuture<GridRestResponse> f) {
                                try {
                                    fut.onDone(f.get());
                                }
                                catch (IgniteCheckedException e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Client request execution failed with error.", e);

                        fut.onDone(U.cast(e));

                        if (e instanceof Error)
                            throw e;
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                ctx.getRestExecutorService().execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on REST executor service). " +
                    "Will attempt to process request in the current thread instead.", e);

                w.run();
            }

            return fut;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridRestResponse> handleRequest(final GridRestRequest req) {
        if (startLatch.getCount() > 0) {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to handle request " +
                    "(protocol handler was interrupted when awaiting grid start).", e));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Received request from client: " + req);

        if (ctx.security().enabled()) {
            Session ses;

            try {
                ses = session(req);
            }
            catch (IgniteCheckedException e) {
                GridRestResponse res = new GridRestResponse(STATUS_FAILED, e.getMessage());

                return new GridFinishedFuture<>(res);
            }

            assert ses != null;

            req.clientId(ses.clientId);
            req.sessionToken(U.uuidToBytes(ses.sesId));

            if (log.isDebugEnabled())
                log.debug("Next clientId and sessionToken were extracted according to request: " +
                    "[clientId="+req.clientId()+", sessionToken="+Arrays.toString(req.sessionToken())+"]");

            try {
                if (ses.secCtx == null)
                    ses.secCtx = authenticate(req);

                authorize(req, ses.secCtx);
            }
            catch (SecurityException e) {
                assert ses.secCtx != null;

                GridRestResponse res = new GridRestResponse(STATUS_SECURITY_CHECK_FAILED, e.getMessage());

                return new GridFinishedFuture<>(res);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(new GridRestResponse(STATUS_AUTH_FAILED, e.getMessage()));
            }
        }

        interceptRequest(req);

        GridRestCommandHandler hnd = handlers.get(req.command());

        IgniteInternalFuture<GridRestResponse> res = hnd == null ? null : hnd.handleAsync(req);

        if (res == null)
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to find registered handler for command: " + req.command()));

        return res.chain(new C1<IgniteInternalFuture<GridRestResponse>, GridRestResponse>() {
            @Override public GridRestResponse apply(IgniteInternalFuture<GridRestResponse> f) {
                GridRestResponse res;

                try {
                    res = f.get();
                }
                catch (Exception e) {
                    if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                        LT.error(log, e, "Failed to handle request: " + req.command());

                    if (log.isDebugEnabled())
                        log.debug("Failed to handle request [req=" + req + ", e=" + e + "]");

                    res = new GridRestResponse(STATUS_FAILED, e.getMessage());
                }

                assert res != null;

                if (ctx.security().enabled())
                    res.sessionTokenBytes(req.sessionToken());

                interceptResponse(res, req);

                return res;
            }
        });
    }

    /**
     * @param req Request.
     * @return Not null session.
     * @throws IgniteCheckedException If failed.
     */
    private Session session(final GridRestRequest req) throws IgniteCheckedException {
        final UUID clientId = req.clientId();
        final byte[] sesTok = req.sessionToken();

        for (int i = 0; i < 10_000; i++) {
            if (F.isEmpty(sesTok) && clientId == null) {
                Session ses = Session.random();

                if (clientId2SesId.putIfAbsent(ses.clientId, ses.sesId) != null)
                    continue; /** New random clientId equals to existing clientId */

                sesId2Ses.put(ses.sesId, ses);

                return ses;
            }

            if (F.isEmpty(sesTok) && clientId != null) {
                UUID sesId = clientId2SesId.get(clientId);

                if (sesId == null) {
                    Session ses = Session.fromClientId(clientId);

                    if (clientId2SesId.putIfAbsent(ses.clientId, ses.sesId) != null)
                        continue; /** Another thread already register session with the clientId. */

                    sesId2Ses.put(ses.sesId, ses);

                    return ses;
                }
                else {
                    Session ses = sesId2Ses.get(sesId);

                    if (ses == null)
                        continue; /** Need to wait while timeout thread complete removing of timed out sessions. */

                    if (ses.checkTimeoutAndTryUpdateLastTouchTime())
                        continue; /** Need to wait while timeout thread complete removing of timed out sessions. */

                    return ses;
                }
            }

            if (!F.isEmpty(sesTok) && clientId == null) {
                UUID sesId = U.bytesToUuid(sesTok, 0);

                Session ses = sesId2Ses.get(sesId);

                if (ses == null)
                    throw new IgniteCheckedException("Failed to handle request. Unknown session token " +
                        "(maybe expired session). [sessionToken=" + U.byteArray2HexString(sesTok) + "]");

                if (ses.checkTimeoutAndTryUpdateLastTouchTime())
                    continue; /** Need to wait while timeout thread complete removing of timed out sessions. */

                return ses;
            }

            if (!F.isEmpty(sesTok) && clientId != null) {
                UUID sesId = clientId2SesId.get(clientId);

                if (sesId == null || !sesId.equals(U.bytesToUuid(sesTok, 0)))
                    throw new IgniteCheckedException("Failed to handle request. " +
                        "Unsupported case (misamatched clientId and session token)");

                Session ses = sesId2Ses.get(sesId);

                if (ses == null)
                    throw new IgniteCheckedException("Failed to handle request. Unknown session token " +
                        "(maybe expired session). [sessionToken=" + U.byteArray2HexString(sesTok) + "]");

                if (ses.checkTimeoutAndTryUpdateLastTouchTime())
                    continue; /** Lets try to reslove session again. */

                return ses;
            }

            throw new IgniteCheckedException("Failed to handle request (Unreachable state).");
        }

        throw new IgniteCheckedException("Failed to handle request (Could not resolve session).");
    }

    /**
     * @param ctx Context.
     */
    public GridRestProcessor(GridKernalContext ctx) {
        super(ctx);

        long sesExpTime0;

        try {
            String sesExpTime = System.getProperty(IgniteSystemProperties.IGNITE_REST_SESSION_TIMEOUT);

            if (sesExpTime != null)
                sesExpTime0 = Long.valueOf(sesExpTime) * 1000;
            else
                sesExpTime0 = DEFAULT_SES_TIMEOUT;
        }
        catch (NumberFormatException ignore) {
            sesExpTime0 = DEFAULT_SES_TIMEOUT;
        }

        sesTtl = sesExpTime0;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isRestEnabled()) {
            // Register handlers.
            addHandler(new GridCacheCommandHandler(ctx));
            addHandler(new GridTaskCommandHandler(ctx));
            addHandler(new GridTopologyCommandHandler(ctx));
            addHandler(new GridVersionCommandHandler(ctx));
            addHandler(new DataStructuresCommandHandler(ctx));
            addHandler(new QueryCommandHandler(ctx));

            // Start protocols.
            startTcpProtocol();
            startHttpProtocol();

            for (GridRestProtocol proto : protos) {
                Collection<IgniteBiTuple<String, Object>> props = proto.getProperties();

                if (props != null) {
                    for (IgniteBiTuple<String, Object> p : props) {
                        String key = p.getKey();

                        if (key == null)
                            continue;

                        if (ctx.hasNodeAttribute(key))
                            throw new IgniteCheckedException(
                                "Node attribute collision for attribute [processor=GridRestProcessor, attr=" + key + ']');

                        ctx.addNodeAttribute(key, p.getValue());
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        Thread sesTimeoutCheckerThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while(!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(SES_TIMEOUT_CHECK_DELAY);

                        for (Iterator<Map.Entry<UUID, Session>> iter = sesId2Ses.entrySet().iterator();
                            iter.hasNext();) {
                            Map.Entry<UUID, Session> e = iter.next();

                            Session ses = e.getValue();

                            if (ses.checkTimeout(sesTtl)) {
                                iter.remove();

                                clientId2SesId.remove(ses.clientId, ses.sesId);
                            }
                        }
                    }
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "session-timeout-checker");

        sesTimeoutCheckerThread.setDaemon(true);

        sesTimeoutCheckerThread.start();

        if (isRestEnabled()) {
            for (GridRestProtocol proto : protos)
                proto.onKernalStart();

            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor started.");
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop(boolean cancel) {
        if (isRestEnabled()) {
            busyLock.writeLock();

            boolean interrupted = Thread.interrupted();

            while (workersCnt.sum() != 0) {
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted)
                Thread.currentThread().interrupt();

            for (GridRestProtocol proto : protos)
                proto.stop();

            // Safety.
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor stopped.");
        }
    }

    /**
     * Applies {@link ConnectorMessageInterceptor}
     * from {@link ConnectorConfiguration#getMessageInterceptor()} ()}
     * to all user parameters in the request.
     *
     * @param req Client request.
     */
    private void interceptRequest(GridRestRequest req) {
        ConnectorMessageInterceptor interceptor = config().getMessageInterceptor();

        if (interceptor == null)
            return;

        if (req instanceof GridRestCacheRequest) {
            GridRestCacheRequest req0 = (GridRestCacheRequest) req;

            req0.key(interceptor.onReceive(req0.key()));
            req0.value(interceptor.onReceive(req0.value()));
            req0.value2(interceptor.onReceive(req0.value2()));

            Map<Object, Object> oldVals = req0.values();

            if (oldVals != null) {
                Map<Object, Object> newVals = U.newHashMap(oldVals.size());

                for (Map.Entry<Object, Object> e : oldVals.entrySet())
                    newVals.put(interceptor.onReceive(e.getKey()), interceptor.onReceive(e.getValue()));

                req0.values(U.sealMap(newVals));
            }
        }
        else if (req instanceof GridRestTaskRequest) {
            GridRestTaskRequest req0 = (GridRestTaskRequest) req;

            List<Object> oldParams = req0.params();

            if (oldParams != null) {
                Collection<Object> newParams = new ArrayList<>(oldParams.size());

                for (Object o : oldParams)
                    newParams.add(interceptor.onReceive(o));

                req0.params(U.sealList(newParams));
            }
        }
    }

    /**
     * Applies {@link ConnectorMessageInterceptor} from
     * {@link ConnectorConfiguration#getMessageInterceptor()}
     * to all user objects in the response.
     *
     * @param res Response.
     * @param req Request.
     */
    private void interceptResponse(GridRestResponse res, GridRestRequest req) {
        ConnectorMessageInterceptor interceptor = config().getMessageInterceptor();

        if (interceptor != null && res.getResponse() != null) {
            switch (req.command()) {
                case CACHE_CONTAINS_KEYS:
                case CACHE_CONTAINS_KEY:
                case CACHE_GET:
                case CACHE_GET_ALL:
                case CACHE_PUT:
                case CACHE_ADD:
                case CACHE_PUT_ALL:
                case CACHE_REMOVE:
                case CACHE_REMOVE_ALL:
                case CACHE_REPLACE:
                case ATOMIC_INCREMENT:
                case ATOMIC_DECREMENT:
                case CACHE_CAS:
                case CACHE_APPEND:
                case CACHE_PREPEND:
                    res.setResponse(interceptSendObject(res.getResponse(), interceptor));

                    break;

                case EXE:
                    if (res.getResponse() instanceof GridClientTaskResultBean) {
                        GridClientTaskResultBean taskRes = (GridClientTaskResultBean)res.getResponse();

                        taskRes.setResult(interceptor.onSend(taskRes.getResult()));
                    }

                    break;

                default:
                    break;
            }
        }
    }

    /**
     * Applies interceptor to a response object.
     * Specially handler {@link Map} and {@link Collection} responses.
     *
     * @param obj Response object.
     * @param interceptor Interceptor to apply.
     * @return Intercepted object.
     */
    private static Object interceptSendObject(Object obj, ConnectorMessageInterceptor interceptor) {
        if (obj instanceof Map) {
            Map<Object, Object> original = (Map<Object, Object>)obj;

            Map<Object, Object> m = new HashMap<>();

            for (Map.Entry e : original.entrySet())
                m.put(interceptor.onSend(e.getKey()), interceptor.onSend(e.getValue()));

            return m;
        }
        else if (obj instanceof Collection) {
            Collection<Object> original = (Collection<Object>)obj;

            Collection<Object> c = new ArrayList<>(original.size());

            for (Object e : original)
                c.add(interceptor.onSend(e));

            return c;
        }
        else
            return interceptor.onSend(obj);
    }

    /**
     * Authenticates remote client.
     *
     * @param req Request to authenticate.
     * @return Authentication subject context.
     * @throws IgniteCheckedException If authentication failed.
     */
    private SecurityContext authenticate(GridRestRequest req) throws IgniteCheckedException {
        assert req.clientId() != null;

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(req.clientId());

        SecurityCredentials cred;

        if (req.credentials() instanceof SecurityCredentials)
            cred = (SecurityCredentials)req.credentials();
        else if (req.credentials() instanceof String) {
            String credStr = (String)req.credentials();

            int idx = credStr.indexOf(':');

            cred = idx >= 0 && idx < credStr.length() ?
                new SecurityCredentials(credStr.substring(0, idx), credStr.substring(idx + 1)) :
                new SecurityCredentials(credStr, null);
        }
        else {
            cred = new SecurityCredentials();

            cred.setUserObject(req.credentials());
        }

        authCtx.address(req.address());

        authCtx.credentials(cred);

        SecurityContext subjCtx = ctx.security().authenticate(authCtx);

        if (subjCtx == null) {
            if (req.credentials() == null)
                throw new IgniteCheckedException("Failed to authenticate remote client (secure session SPI not set?): " + req);
            else
                throw new IgniteCheckedException("Failed to authenticate remote client (invalid credentials?): " + req);
        }

        return subjCtx;
    }

    /**
     * @param req REST request.
     * @param sCtx Security context.
     * @throws SecurityException If authorization failed.
     */
    private void authorize(GridRestRequest req, SecurityContext sCtx) throws SecurityException {
        SecurityPermission perm = null;
        String name = null;

        switch (req.command()) {
            case CACHE_GET:
            case CACHE_CONTAINS_KEY:
            case CACHE_CONTAINS_KEYS:
            case CACHE_GET_ALL:
                perm = SecurityPermission.CACHE_READ;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY:
            case CLOSE_SQL_QUERY:
            case FETCH_SQL_QUERY:
                perm = SecurityPermission.CACHE_READ;
                name = ((RestSqlQueryRequest)req).cacheName();

                break;

            case CACHE_PUT:
            case CACHE_ADD:
            case CACHE_PUT_ALL:
            case CACHE_REPLACE:
            case CACHE_CAS:
            case CACHE_APPEND:
            case CACHE_PREPEND:
            case CACHE_GET_AND_PUT:
            case CACHE_GET_AND_REPLACE:
            case CACHE_GET_AND_PUT_IF_ABSENT:
            case CACHE_PUT_IF_ABSENT:
            case CACHE_REPLACE_VALUE:
                perm = SecurityPermission.CACHE_PUT;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_REMOVE:
            case CACHE_REMOVE_ALL:
            case CACHE_GET_AND_REMOVE:
            case CACHE_REMOVE_VALUE:
                perm = SecurityPermission.CACHE_REMOVE;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case EXE:
            case RESULT:
                perm = SecurityPermission.TASK_EXECUTE;
                name = ((GridRestTaskRequest)req).taskName();

                break;

            case GET_OR_CREATE_CACHE:
            case DESTROY_CACHE:
                perm = SecurityPermission.ADMIN_CACHE;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_METRICS:
            case CACHE_SIZE:
            case TOPOLOGY:
            case NODE:
            case VERSION:
            case NOOP:
            case QUIT:
            case ATOMIC_INCREMENT:
            case ATOMIC_DECREMENT:
            case NAME:
            case LOG:
                break;

            default:
                throw new AssertionError("Unexpected command: " + req.command());
        }

        if (perm != null)
            ctx.security().authorize(name, perm, sCtx);
    }

    /**
     *
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        return !ctx.config().isDaemon() && ctx.config().getConnectorConfiguration() != null;
    }

    /**
     * @param hnd Command handler.
     */
    private void addHandler(GridRestCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added REST command handler: " + hnd);

        for (GridRestCommand cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd) : cmd;

            handlers.put(cmd, hnd);
        }
    }

    /**
     * Starts TCP protocol.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private void startTcpProtocol() throws IgniteCheckedException {
        startProtocol(new GridTcpRestProtocol(ctx));
    }

    /**
     * Starts HTTP protocol if it exists on classpath.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private void startHttpProtocol() throws IgniteCheckedException {
        try {
            Class<?> cls = Class.forName(HTTP_PROTO_CLS);

            Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

            GridRestProtocol proto = (GridRestProtocol)ctor.newInstance(ctx);

            startProtocol(proto);
        }
        catch (ClassNotFoundException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to initialize HTTP REST protocol (consider adding ignite-rest-http " +
                    "module to classpath).");
        }
        catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to initialize HTTP REST protocol.", e);
        }
    }

    /**
     * @return Client configuration.
     */
    private ConnectorConfiguration config() {
        return ctx.config().getConnectorConfiguration();
    }

    /**
     * @param proto Protocol.
     * @throws IgniteCheckedException If protocol initialization failed.
     */
    private void startProtocol(GridRestProtocol proto) throws IgniteCheckedException {
        assert proto != null;
        assert !protos.contains(proto);

        protos.add(proto);

        proto.start(protoHnd);

        if (log.isDebugEnabled())
            log.debug("Added REST protocol: " + proto);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> REST processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   protosSize: " + protos.size());
        X.println(">>>   handlersSize: " + handlers.size());
    }

    /**
     * Session.
     */
    private static class Session {
        /** Expiration flag. It's a final state of lastToucnTime. */
        private static final Long TIMEDOUT_FLAG = 0L;

        /** Client id. */
        private final UUID clientId;

        /** Session token id. */
        private final UUID sesId;

        /** Security context. */
        private volatile SecurityContext secCtx;

        /**
         * Time when session is used last time.
         * If this time was set at TIMEDOUT_FLAG, then it should never be changed.
         */
        private final AtomicLong lastTouchTime = new AtomicLong(U.currentTimeMillis());

        /**
         * @param clientId Client ID.
         * @param sesId session ID.
         */
        private Session(UUID clientId, UUID sesId) {
            this.clientId = clientId;
            this.sesId = sesId;
        }

        /**
         * Static constructor.
         *
         * @return New session instance with random client ID and random session ID.
         */
        static Session random() {
            return new Session(UUID.randomUUID(), UUID.randomUUID());
        }

        /**
         * Static constructor.
         *
         * @param clientId Client ID.
         * @return New session instance with given client ID and random session ID.
         */
        static Session fromClientId(UUID clientId) {
            return new Session(clientId, UUID.randomUUID());
        }

        /**
         * Static constructor.
         *
         * @param sesTokId Session token ID.
         * @return New session instance with random client ID and given session ID.
         */
        static Session fromSessionToken(UUID sesTokId) {
            return new Session(UUID.randomUUID(), sesTokId);
        }

        /**
         * Checks expiration of session and if expired then sets TIMEDOUT_FLAG.
         *
         * @param sesTimeout Session timeout.
         * @return <code>True</code> if expired.
         * @see #checkTimeoutAndTryUpdateLastTouchTime()
         */
        boolean checkTimeout(long sesTimeout) {
            long time0 = lastTouchTime.get();

            if (U.currentTimeMillis() - time0 > sesTimeout)
                lastTouchTime.compareAndSet(time0, TIMEDOUT_FLAG);

            return isTimedOut();
        }

        /**
         * Checks whether session at expired state (EPIRATION_FLAG) or not, if not then tries to update last touch time.
         *
         * @return <code>True</code> if timed out.
         * @see #checkTimeout(long)
         */
        boolean checkTimeoutAndTryUpdateLastTouchTime() {
            while (true) {
                long time0 = lastTouchTime.get();

                if (time0 == TIMEDOUT_FLAG)
                    return true;

                boolean success = lastTouchTime.compareAndSet(time0, U.currentTimeMillis());

                if (success)
                    return false;
            }
        }

        /**
         * @return <code>True</code> if session in expired state.
         */
        boolean isTimedOut() {
            return lastTouchTime.get() == TIMEDOUT_FLAG;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Session))
                return false;

            Session ses = (Session)o;

            if (clientId != null ? !clientId.equals(ses.clientId) : ses.clientId != null)
                return false;
            if (sesId != null ? !sesId.equals(ses.sesId) : ses.sesId != null)
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = clientId != null ? clientId.hashCode() : 0;
            res = 31 * res + (sesId != null ? sesId.hashCode() : 0);
            return res;
        }
    }
}
