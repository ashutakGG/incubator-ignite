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

package org.apache.ignite;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;

/**
 * TODO: Add class description.
 */
public class TestHangsTmp extends GridCacheAbstractSelfTest {
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        if (gridName.endsWith("2") || gridName.endsWith("3"))
            c.setClientMode(true);
//
        CacheConfiguration cc1 = txCache("tx-write-invoke");

        CacheConfiguration cc2 = txCache("tx-invoke-retry");

        CacheConfiguration[] ccfg = c.getCacheConfiguration();

        c.setCacheConfiguration(ccfg[0], cc1, cc2);

        return c;
    }
    
    private CacheConfiguration txCache(String name) {
        CacheConfiguration cc1 = new CacheConfiguration();
        cc1.setName(name);
        cc1.setCacheMode(CacheMode.PARTITIONED);
        cc1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc1.setSwapEnabled(false);
        return cc1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testName() throws Exception {


    }
}
