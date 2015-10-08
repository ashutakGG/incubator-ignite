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

package org.apache.ignite.yardstick.cache.failover;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Atomic retries failover benchmark. Client generates continuous load to the cluster (random get, put, invoke, remove
 * operations)
 */
public class IgniteTransactionalInvokeRetryBenchmark extends IgniteFailoverAbstractBenchmark<String, Long> {
    /** */
    public static final int KEY_RANGE = 100_000;

    /** */
    private final ConcurrentMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);

    /** */
    private volatile Exception ex;

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    final int timeout = args.cacheOperationTimeoutMillis();
                    final int keysCnt = args.keysCount();

                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(args.cacheConsistencyCheckingPeriod() * 1000);

                        rwl.writeLock().lock();

                        try {
                            println("[CACHE-VALIDATOR] Start cache validation.");

                            long startTime = U.currentTimeMillis();

                            Map<String, Long> notEqualsCacheVals = new HashMap<>();
                            Map<String, Long> notEqualsLocMapVals = new HashMap<>();

                            for (int k = 0; k < KEY_RANGE; k++) {
                                if (k % 10_000 == 0)
                                    println("[CACHE-VALIDATOR] Start validation for keys like 'key-" + k + "-*'");

                                for (int i = 0; i < keysCnt; i++) {
                                    String key = "key-" + k + "-" + cfg.memberId() + "-" + i;

                                    asyncCache.get(key);
                                    Long cacheVal = asyncCache.<Long>future().get(timeout);

                                    AtomicLong aVal = map.get(key);
                                    Long mapVal = aVal != null ? aVal.get() : null;

                                    if (!Objects.equals(cacheVal, mapVal)) {
                                        notEqualsCacheVals.put(key, cacheVal);
                                        notEqualsLocMapVals.put(key, mapVal);
                                    }
                                }
                            }

                            assert notEqualsCacheVals.size() == notEqualsLocMapVals.size(): "Cache map = "
                                + notEqualsCacheVals + ", map vals = " + notEqualsLocMapVals;

                            if (!notEqualsCacheVals.isEmpty()) {
                                // Print all usefull information and finish.
                                for (Map.Entry<String, Long> eLocMap : notEqualsLocMapVals.entrySet()) {
                                    String key = eLocMap.getKey();
                                    Long mapVal = eLocMap.getValue();
                                    Long cacheVal = notEqualsCacheVals.get(key);

                                    println(cfg, "[CACHE-VALIDATOR] Got different values [key='" + key
                                        + "', cacheVal=" + cacheVal + ", localMapVal=" + mapVal + "]");
                                }

                                println(cfg, "Local driver map contant:\n " + map);

                                println(cfg, "Cache content:");

                                for (int k2 = 0; k2 < KEY_RANGE; k2++) {
                                    for (int i2 = 0; i2 < keysCnt; i2++) {
                                        String key2 = "key-" + k2 + "-" + cfg.memberId() + "-" + i2;

                                        asyncCache.get(key2);
                                        Long val = asyncCache.<Long>future().get(timeout);

                                        if (val != null)
                                            println(cfg, "Entry [key=" + key2 + ", val=" + val + "]");
                                    }
                                }

                                U.dumpThreads(null);

                                throw new IllegalStateException("Cache and local map are in inconsistent state.");
                            }

                            println("[CACHE-VALIDATOR] Cache validation successfully finished in "
                                + (U.currentTimeMillis() - startTime) / 1000 + " sec.");
                        }
                        finally {
                            rwl.writeLock().unlock();
                        }
                    }
                }
                catch (Throwable e) {
                    ex = new Exception(e);

                    println("[CACHE-VALIDATOR] Got exception: " + e);

                    e.printStackTrace();

                    U.dumpThreads(null);

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }
        }, "cache-validator");

        thread.setDaemon(true);

        thread.start();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int k = nextRandom(KEY_RANGE);

        final String[] keys = new String[args.keysCount()];

        assert keys.length > 0 : "Count of keys = " + keys.length;

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + cfg.memberId() + "-" + i;

        for (String key : keys) {
            rwl.readLock().lock();

            try {
                if (ex != null)
                    throw ex;

                asyncCache.invoke(key, new IncrementCacheEntryProcessor());
                asyncCache.future().get(args.cacheOperationTimeoutMillis());

                AtomicLong prevVal = map.putIfAbsent(key, new AtomicLong(0));

                if (prevVal != null)
                    prevVal.incrementAndGet();
            }
            finally {
                rwl.readLock().unlock();
            }
        }

        if (ex != null)
            throw ex;

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Long> cache() {
        return ignite().cache("atomic");
    }

    /**
     */
    private static class IncrementCacheEntryProcessor implements CacheEntryProcessor<String, Long, Long> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<String, Long> entry,
            Object... arguments) throws EntryProcessorException {
            long newVal = entry.getValue() == null ? 0 : entry.getValue() + 1;

            entry.setValue(newVal);

            return newVal;
        }
    }
}
