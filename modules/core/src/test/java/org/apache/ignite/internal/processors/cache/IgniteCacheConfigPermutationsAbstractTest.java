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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.R1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.CacheStartMode;
import org.apache.ignite.testframework.junits.IgniteConfigPermutationsAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;

/**
 * Abstract class for cache tests.
 */
public abstract class IgniteCacheConfigPermutationsAbstractTest extends IgniteConfigPermutationsAbstractTest {
    /** */
    protected static final int CLIENT_NEAR_ONLY_IDX = 2;

    /** Test timeout */
    private static final long TEST_TIMEOUT = 30 * 1000;

    /** Store map. */
    protected static final Map<Object, Object> map = new ConcurrentHashMap8<>();

    /** VM ip finder for TCP discovery. */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected final void beforeTestsStarted() throws Exception {
        assert testsCfg != null;
        assert !testsCfg.withClients() || testsCfg.gridCount() >= 3;

        assert testsCfg.testedNodeIndex() >= 0 : "testedNodeIdx: " + testedNodeIdx;

        testedNodeIdx = testsCfg.testedNodeIndex();

        if (testsCfg.isStartCache()) {
            final CacheStartMode cacheStartMode = testsCfg.cacheStartMode();
            final int cnt = testsCfg.gridCount();

            if (cacheStartMode == CacheStartMode.STATIC) {
                info("All nodes will be stopped, new " + cnt + " nodes will be started.");

                Ignition.stopAll(true);

                for (int i = 0; i < cnt; i++) {
                    String gridName = getTestGridName(i);

                    IgniteConfiguration cfg = optimize(getConfiguration(gridName));


                    if (i != CLIENT_NODE_IDX && i != CLIENT_NEAR_ONLY_IDX) {
                        CacheConfiguration cc = testsCfg.configurationFactory().cacheConfiguration(gridName);

                        cc.setName(cacheName());

                        cfg.setCacheConfiguration(cc);
                    }

                    startGrid(gridName, cfg, null);
                }

                if (testsCfg.withClients() && testsCfg.gridCount() > CLIENT_NEAR_ONLY_IDX)
                    grid(CLIENT_NEAR_ONLY_IDX).createNearCache(cacheName(), new NearCacheConfiguration());
            }
            else if (cacheStartMode == null || cacheStartMode == CacheStartMode.NODES_THEN_CACHES) {
                super.beforeTestsStarted();

                for (int i = 0; i < gridCount(); i++) {
                    info("Starting cache dinamically on grid: " + i);

                    IgniteEx grid = grid(i);

                    if (i != CLIENT_NODE_IDX && i != CLIENT_NEAR_ONLY_IDX) {
                        CacheConfiguration cc = testsCfg.configurationFactory().cacheConfiguration(grid.name());

                        cc.setName(cacheName());

                        grid.getOrCreateCache(cc);
                    }

                    if (testsCfg.withClients() && testsCfg.gridCount() > CLIENT_NEAR_ONLY_IDX)
                        grid(CLIENT_NEAR_ONLY_IDX).createNearCache(cacheName(), new NearCacheConfiguration());
                }
            }
            else
                throw new IllegalArgumentException("Unknown cache start mode: " + cacheStartMode);
        }

        if (testsCfg.gridCount() > 1)
            checkTopology(testsCfg.gridCount());

        awaitPartitionMapExchange();

        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());

        if (testsCfg.withClients()) {
            boolean testedNodeNearEnabled = grid(testedNodeIdx).cachex(cacheName()).context().isNear();

            if (testedNodeIdx != SERVER_NODE_IDX)
                assertEquals(testedNodeIdx == CLIENT_NEAR_ONLY_IDX, testedNodeNearEnabled);

            info(">>> Starting set of tests [testedNodeIdx=" + testedNodeIdx
                + ", id=" + grid(testedNodeIdx).localNode().id()
                + ", isClient=" + grid(testedNodeIdx).configuration().isClientMode()
                + ", nearEnabled=" + testedNodeNearEnabled + "]");
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        return getTestGridName(CLIENT_NODE_IDX).equals(testGridName) 
            || getTestGridName(CLIENT_NEAR_ONLY_IDX).equals(testGridName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (testsCfg.isStopCache()) {
            for (int i = 0; i < gridCount(); i++) {
                info("Destroing cache on grid: " + i);

                IgniteCache<String, Integer> cache = jcache(i);

                assert i != 0 || cache != null;

                if (cache != null)
                    cache.destroy();
            }
        }

        map.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        awaitPartitionMapExchange();

        assert jcache().unwrap(Ignite.class).transactions().tx() == null;
        assertEquals(0, jcache().localSize());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Transaction tx = jcache().unwrap(Ignite.class).transactions().tx();

        if (tx != null) {
            tx.close();

            fail("Cache transaction remained after test completion: " + tx);
        }

        for (int i = 0; i < gridCount(); i++) {
            info("Checking grid: " + i);

            while (true) {
                try {
                    jcache(i).removeAll();
//                    final int fi = i;
//
//                    assertTrue(
//                        "Cache is not empty: " + " localSize = " + jcache(fi).localSize(CachePeekMode.ALL)
//                        + ", local entries " + entrySet(jcache(fi).localEntries()),
//                        GridTestUtils.waitForCondition(
//                            // Preloading may happen as nodes leave, so we need to wait.
//                            new GridAbsPredicateX() {
//                                @Override public boolean applyx() throws IgniteCheckedException {
//                                    jcache(fi).removeAll();
//
//                                    if (jcache(fi).size(CachePeekMode.ALL) > 0) {
//                                        for (Cache.Entry<?, ?> k : jcache(fi).localEntries())
//                                            jcache(fi).remove(k.getKey());
//                                    }
//
//                                    int locSize = jcache(fi).localSize(CachePeekMode.ALL);
//
//                                    if (locSize != 0) {
//                                        info(">>>>> Debug localSize for grid: " + fi + " is " + locSize);
//                                        info(">>>>> Debug ONHEAP  localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.ONHEAP));
//                                        info(">>>>> Debug OFFHEAP localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.OFFHEAP));
//                                        info(">>>>> Debug PRIMARY localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.PRIMARY));
//                                        info(">>>>> Debug BACKUP  localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.BACKUP));
//                                        info(">>>>> Debug NEAR    localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.NEAR));
//                                        info(">>>>> Debug SWAP    localSize for grid: " + fi + " is " + jcache(fi).localSize(CachePeekMode.SWAP));
//                                    }
//
//                                    return locSize == 0;
//                                }
//                            },
//                            getTestTimeout()));

                    int primaryKeySize = jcache(i).localSize(CachePeekMode.PRIMARY);
                    int keySize = jcache(i).localSize();
                    int size = jcache(i).localSize();
                    int globalSize = jcache(i).size();
                    int globalPrimarySize = jcache(i).size(CachePeekMode.PRIMARY);

                    info("Size after [idx=" + i +
                        ", size=" + size +
                        ", keySize=" + keySize +
                        ", primarySize=" + primaryKeySize +
                        ", globalSize=" + globalSize +
                        ", globalPrimarySize=" + globalPrimarySize +
                        ", entrySet=" + jcache(i).localEntries() + ']');

//                    assertEquals("Cache is not empty [idx=" + i + ", entrySet=" + jcache(i).localEntries() + ']',
//                        0, jcache(i).localSize(CachePeekMode.ALL));

                    break;
                }
                catch (Exception e) {
                    if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                        info("Got topology exception while tear down (will retry in 1000ms).");

                        U.sleep(1000);
                    }
                    else
                        throw e;
                }
            }

            for (Cache.Entry entry : jcache(i).localEntries(CachePeekMode.SWAP))
                jcache(i).remove(entry.getKey());
        }

        assert jcache().unwrap(Ignite.class).transactions().tx() == null;
//        assertEquals("Cache is not empty", 0, jcache().localSize(CachePeekMode.ALL));

        resetStore();
    }

    /**
     * Cleans up cache store.
     */
    protected void resetStore() {
        map.clear();
    }

    /**
     * Put entry to cache store.
     *
     * @param key Key.
     * @param val Value.
     */
    protected void putToStore(Object key, Object val) {
        if (!storeEnabled())
            throw new IllegalStateException("Failed to put to store because store is disabled.");

        map.put(key, val);
    }

    /**
     * Indexed types.
     */
    protected Class<?>[] indexedTypes() {
        return null;
    }

    /**
     * @return Default cache mode.
     */
    protected CacheMode cacheMode() {
        CacheMode mode = cacheConfiguration().getCacheMode();

        return mode == null ? CacheConfiguration.DFLT_CACHE_MODE : mode;
    }

    /**
     * @return Load previous value flag.
     */
    protected boolean isLoadPreviousValue() {
        return cacheConfiguration().isLoadPreviousValue();
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return cacheConfiguration().getAtomicityMode();
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected CacheMemoryMode memoryMode() {
        return cacheConfiguration().getMemoryMode();
    }

    /**
     * @return {@code True} if swap should happend after localEvict() call.
     */
    protected boolean swapAfterLocalEvict() {
        if (memoryMode() == OFFHEAP_TIERED)
            return false;

        return memoryMode() == ONHEAP_TIERED ? (!offheapEnabled() && swapEnabled()) : swapEnabled();
    }

    /**
     * @return {@code True} if store is enabled.
     */
    protected boolean storeEnabled() {
        return cacheConfiguration().getCacheStoreFactory() != null;
    }

    /**
     * @return {@code True} if offheap memory is enabled.
     */
    protected boolean offheapEnabled() {
        return cacheConfiguration().getOffHeapMaxMemory() >= 0;
    }

    /**
     * @return {@code True} if swap is enabled.
     */
    protected boolean swapEnabled() {
        return cacheConfiguration().isSwapEnabled();
    }

    /**
     * @return Partitioned mode.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return cacheConfiguration().getNearConfiguration();
    }

    /**
     * @return Write through storage emulator.
     */
    public static CacheStore<?, ?> cacheStore() {
        return new CacheStoreAdapter<Object, Object>() {
            @Override public void loadCache(IgniteBiInClosure<Object, Object> clo,
                Object... args) {
                for (Map.Entry<Object, Object> e : map.entrySet())
                    clo.apply(e.getKey(), e.getValue());
            }

            @Override public Object load(Object key) {
                return map.get(key);
            }

            @Override public void write(Cache.Entry<? extends Object, ? extends Object> e) {
                map.put(e.getKey(), e.getValue());
            }

            @Override public void delete(Object key) {
                map.remove(key);
            }
        };
    }

    /**
     * @return {@code true} if near cache should be enabled.
     */
    protected boolean nearEnabled() {
        return nearConfiguration() != null;
    }

    /**
     * @return {@code True} if transactions are enabled.
     * @see #txShouldBeUsed()
     */
    protected boolean txEnabled() {
        return atomicityMode() == TRANSACTIONAL;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        return testsCfg.configurationFactory().cacheConfiguration(getTestGridName(testedNodeIdx));
    }

    /**
     * @return {@code True} if transactions should be used.
     */
    protected boolean txShouldBeUsed() {
        return txEnabled() && !isMultiJvm();
    }

    /**
     * @return {@code True} if locking is enabled.
     */
    protected boolean lockingEnabled() {
        return txEnabled();
    }

    /**
     * @return Default cache instance.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected <K, V> IgniteCache<K, V> jcache() {
        return jcache(testedNodeIdx);
    }

    /**
     * @return A not near-only cache.
     */
    protected IgniteCache<String, Integer> serverNodeCache() {
        return jcache(SERVER_NODE_IDX);
    }

    /**
     * @return Cache name.
     */
    protected String cacheName() {
        return "testcache-" + testsCfg.description().hashCode();
    }

    /**
     * @return Transactions instance.
     */
    protected IgniteTransactions transactions() {
        return grid(0).transactions();
    }

    /**
     * @param idx Index of grid.
     * @return Default cache.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected <K, V> IgniteCache<K, V> jcache(int idx) {
        return ignite(idx).cache(cacheName());
    }

    /**
     * @param idx Index of grid.
     * @return Cache context.
     */
    protected GridCacheContext<String, Integer> context(final int idx) {
        if (isRemoteJvm(idx) && !isRemoteJvm())
            throw new UnsupportedOperationException("Operation can't be done automatically via proxy. " +
                "Send task with this logic on remote jvm instead.");

        return ((IgniteKernal)grid(idx)).<String, Integer>internalCache(cacheName()).context();
    }

    /**
     * @param key Key.
     * @param idx Node index.
     * @return {@code True} if key belongs to node with index idx.
     */
    protected boolean belongs(String key, int idx) {
        return context(idx).cache().affinity().isPrimaryOrBackup(context(idx).localNode(), key);
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache has OFFHEAP_TIERED memory mode.
     */
    protected static <K, V> boolean offheapTiered(IgniteCache<K, V> cache) {
        return cache.getConfiguration(CacheConfiguration.class).getMemoryMode() == OFFHEAP_TIERED;
    }

    /**
     * Executes regular peek or peek from swap.
     *
     * @param cache Cache projection.
     * @param key Key.
     * @return Value.
     */
    @Nullable protected static <K, V> V peek(IgniteCache<K, V> cache, K key) {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.SWAP, CachePeekMode.OFFHEAP) :
            cache.localPeek(key, CachePeekMode.ONHEAP);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code True} if cache contains given key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected static boolean containsKey(IgniteCache cache, Object key) throws Exception {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.OFFHEAP) != null : cache.containsKey(key);
    }

    /**
     * Filters cache entry projections leaving only ones with keys containing 'key'.
     */
    protected static IgnitePredicate<Cache.Entry<String, Integer>> entryKeyFilter =
        new P1<Cache.Entry<String, Integer>>() {
        @Override public boolean apply(Cache.Entry<String, Integer> entry) {
            return entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with keys not containing 'key'.
     */
    protected static IgnitePredicate<Cache.Entry<String, Integer>> entryKeyFilterInv =
        new P1<Cache.Entry<String, Integer>>() {
        @Override public boolean apply(Cache.Entry<String, Integer> entry) {
            return !entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with values less than 50.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> lt50 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i < 50;
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 100.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> gte100 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i >= 100;
            }

            @Override public String toString() {
                return "gte100";
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 200.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> gte200 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i >= 200;
            }

            @Override public String toString() {
                return "gte200";
            }
        };

    /**
     * {@link org.apache.ignite.lang.IgniteInClosure} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumVisitor implements CI1<Cache.Entry<String, Integer>> {
        /** */
        private final AtomicInteger sum;

        /**
         * @param sum {@link AtomicInteger} instance for accumulating sum.
         */
        public SumVisitor(AtomicInteger sum) {
            this.sum = sum;
        }

        /** {@inheritDoc} */
        @Override public void apply(Cache.Entry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null : "Value cannot be null for entry: " + entry;

                sum.addAndGet(i);
            }
        }
    }

    /**
     * {@link org.apache.ignite.lang.IgniteReducer} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumReducer implements R1<Cache.Entry<String, Integer>, Integer> {
        /** */
        private int sum;

        /** */
        public SumReducer() {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Cache.Entry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null;

                sum += i;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return sum;
        }
    }

    /**
     * Serializable factory.
     */
    public static class TestStoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            return cacheStore();
        }
    }
}
