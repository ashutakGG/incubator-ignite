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

import java.util.Map;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;

/**
 * Atomic retries failover benchmark. Client generates continuous load to the cluster
 * (random get, put, invoke, remove operations)
 */
public class IgniteAtomicRetriesFailoverBenchmark extends IgniteFailoverAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int key = nextRandom(args.range());

        int opNum = nextRandom(4);

        switch (opNum) {
            case 0:
                cache.get(key);
                break;
            case 1:
                cache.put(key, String.valueOf(key));
                break;
            case 2:
                cache.invoke(key, new CacheEntryProcessor<Integer, Object, Object>() {
                    @Override public Object process(MutableEntry<Integer, Object> entry,
                        Object... arguments) throws EntryProcessorException {
                        return String.valueOf(key);
                    }
                });
                break;
            case 3:
                cache.remove(key);
                break;
            default:
                throw new IllegalStateException("Got opNum = " + opNum);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
