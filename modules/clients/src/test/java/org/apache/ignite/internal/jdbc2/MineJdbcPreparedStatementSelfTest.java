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

package org.apache.ignite.internal.jdbc2;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Prepared statement test.
 */
public class MineJdbcPreparedStatementSelfTest extends JdbcPreparedStatementSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setMarshaller(new BinaryMarshaller());

        // Cache.
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setQueryEntities(Arrays.asList(activityQueryEntity()));

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Create SQL Query descriptor for ACTIVITY.
     *
     * @return Configured query entity.
     */
    private static QueryEntity activityQueryEntity() {
        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType("ACTIVITY");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", "int");

        qryEntity.setFields(fields);

        return qryEntity;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteBinary binary = grid(0).binary();
        IgniteCache cache = grid(0).cache(null);

        for (int i = 0; i < 100; i++) {
            BinaryObjectBuilder builder = binary.builder("ACTIVITY");

            builder.setField("id", i);
            builder.setField("str", "str" + i);

            BinaryObject bo = builder.build();

            cache.put(i, bo);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryPositive() throws Exception {
        stmt = conn.prepareStatement("select id from ACTIVITY ");

        ResultSet rs = stmt.executeQuery();

        while (rs.next())
            System.out.println(rs.getInt("id"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryNegative() throws Exception {
        stmt = conn.prepareStatement("select id from ACTIVITY where id > 10 ");

        ResultSet rs = stmt.executeQuery();

        while (rs.next())
            System.out.println(rs.getInt("id"));
    }
}
