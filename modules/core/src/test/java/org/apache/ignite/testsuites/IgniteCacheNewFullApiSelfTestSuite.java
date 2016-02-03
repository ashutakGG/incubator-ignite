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

package org.apache.ignite.testsuites;

import java.util.Arrays;
import junit.framework.TestSuite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheNewFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridNewCacheAbstractSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.config.StateConfigurationFactory;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;
import org.apache.ignite.testframework.config.generator.StateIterator;
import org.apache.ignite.testframework.config.params.MarshallerProcessor;
import org.apache.ignite.testframework.config.params.Variants;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test suite for cache API.
 */
public class IgniteCacheNewFullApiSelfTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigurationParameter<IgniteConfiguration>[][] igniteParams = new ConfigurationParameter[][] {
        {new MarshallerProcessor(new BinaryMarshaller()), new MarshallerProcessor(new OptimizedMarshaller(true))},
        Variants.booleanVariants("setPeerClassLoadingEnabled"),
    };

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigurationParameter<CacheConfiguration>[][] cacheParams = new ConfigurationParameter[][] {
        Variants.enumVariants(CacheMode.class, "setCacheMode"),
        Variants.enumVariants(CacheAtomicityMode.class, "setAtomicityMode"),
        Variants.enumVariantsWithNull(CacheMemoryMode.class, "setMemoryMode"),
    };

    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache New Full API Test Suite");

        final int[] igniteCfgState = new int[] {0, 0}; // Default configuration.
        final int gridsCnt = 1;

        for (StateIterator cacheIter = new StateIterator(cacheParams); cacheIter.hasNext();) {
            int[] cacheCfgState = cacheIter.next();

            // Stop all grids before starting new ignite configuration.
            addTestSuite(suite, igniteCfgState, cacheCfgState, gridsCnt, !cacheIter.hasNext());
        }

        return suite;
    }

    /**
     * @param suite Suite.
     * @param igniteCfgState Ignite config state.
     * @param cacheCfgState Cache config state.
     * @param gridsCnt Grids count.
     * @param stop Stop.
     */
    private static void addTestSuite(TestSuite suite, int[] igniteCfgState, int[] cacheCfgState, int gridsCnt,
        boolean stop) {
        StateConfigurationFactory factory = new FullApiStateConfigurationFactory(igniteParams, igniteCfgState,
            cacheParams, cacheCfgState);

        String clsNameSuffix = "[igniteCfg=" + Arrays.toString(igniteCfgState)
            + ", cacheCfgState=" + Arrays.toString(cacheCfgState) + "]"
            + "-[igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stop, gridsCnt);

        suite.addTest(new GridTestSuite(GridCacheNewFullApiSelfTest.class, testCfg));
    }

    /**
     * TODO remove it.
     */
    private static class FullApiStateConfigurationFactory extends StateConfigurationFactory {
        /**
         * @param igniteParams Ignite params.
         * @param igniteCfgState Ignite config state.
         * @param cacheParams Cache params.
         * @param cacheCfgState Cache config state.
         */
        FullApiStateConfigurationFactory(
            ConfigurationParameter<IgniteConfiguration>[][] igniteParams, int[] igniteCfgState,
            ConfigurationParameter<CacheConfiguration>[][] cacheParams, int[] cacheCfgState) {
            super(igniteParams, igniteCfgState, cacheParams, cacheCfgState);
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration cacheConfiguration(String gridName) {
            CacheConfiguration cc = super.cacheConfiguration(gridName);

            // Default
            cc.setStartSize(1024);
            cc.setAtomicWriteOrderMode(PRIMARY);
            cc.setNearConfiguration(new NearCacheConfiguration());
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setEvictionPolicy(null);

            // Cache
            CacheStore<?, ?> store = GridNewCacheAbstractSelfTest.cacheStore();

            if (store != null) {
                cc.setCacheStoreFactory(new GridNewCacheAbstractSelfTest.TestStoreFactory());
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
                cc.setLoadPreviousValue(true);
            }

            cc.setSwapEnabled(true);

//            Class<?>[] idxTypes = indexedTypes();
//
//            if (!F.isEmpty(idxTypes))
//                cc.setIndexedTypes(idxTypes);

//            if (cacheMode() == PARTITIONED)
//                cc.setBackups(1);

            // FullApi
            if (cc.getMemoryMode() == OFFHEAP_TIERED || cc.getMemoryMode() == OFFHEAP_VALUES)
                cc.setOffHeapMaxMemory(0);

            return cc;
        }
    }
}
