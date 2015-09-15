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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.jcommander;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs get operations.
 */
public class IgniteFailoverBenchmark extends IgniteCacheAbstractBenchmark {
    /** */
    private final ConcurrentMap<Integer, BenchmarkConfiguration> srvsCfgs = new ConcurrentHashMap<>();

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(">>>>>>>> is client mode = " + ignite().configuration().isClientMode());

        if (cfg.memberId() == 0) {
            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        // Read servers configs from cache and destroy it.
                        IgniteCache<Integer, String[]> srvsCfgsCache = ignite().
                            getOrCreateCache(new CacheConfiguration<Integer, String[]>().setName("serversConfigs"));

                        for (Cache.Entry<Integer, String[]> e : srvsCfgsCache) {
                            Integer serverId = e.getKey();
                            String[] cmdArgs = e.getValue();

                            println("Cache entry id=" + serverId + " args=" + Arrays.toString(cmdArgs));

                            final BenchmarkConfiguration cfg = new BenchmarkConfiguration();

                            cfg.commandLineArguments(cmdArgs);

                            jcommander(cmdArgs, cfg, "<benchmark-runner>");

                            srvsCfgs.put(serverId, cfg);
                        }

                        srvsCfgsCache.destroy();

                        Thread.sleep(20_000);

                        println("srvsCfg map size = " + srvsCfgs.size());

//                        while (!Thread.currentThread().isInterrupted()) {
//                            RestartUtils.kill9("${REMOTE_USER}", "localhost");
//
//                            Thread.sleep(1_000);
//
//                            RestartUtils.start();
//
//                            Thread.sleep(10_000);
//                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "restarter");

            thread.setDaemon(true);

            thread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        cache.get(key);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
