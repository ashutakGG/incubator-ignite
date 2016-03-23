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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings({"unchecked", "PackageVisibleField", "serial"})
public class IgniteCrossCachesJoinsQueryTest extends AbstractH2CompareQueryTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private boolean client;

    /** */
    private Data data;

    /** */
    private String dataAsStr;

    /** */
    private String personCacheName;

    /** */
    private String orgCacheName;

    /** */
    private String accCacheName;

    /** Tested qry. */
    private String qry;

    /** Tested testedCache. */
    private IgniteCache cache;

    /** */
    private boolean distributedJoins;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] cacheConfigurations() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);

        conn = openH2Connection(false);

        initializeH2Schema();
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws SQLException {
        Statement st = conn.createStatement();

        final String keyType = useColocatedDate() ? "other" : "int";

        st.execute("create table \"" + accCacheName + "\".Account" +
            "  (" +
            "  _key " + keyType + " not null," +
            "  _val other not null," +
            "  id int unique," +
            "  personId int," +
            "  personDateId TIMESTAMP," +
            "  personStrId varchar(255)" +
            "  )");

        st.execute("create table \"" + personCacheName + "\".Person" +
            "  (" +
            "  _key " + keyType + " not null," +
            "  _val other not null," +
            "  id int unique," +
            "  strId varchar(255) ," +
            "  dateId TIMESTAMP ," +
            "  orgId int," +
            "  orgDateId TIMESTAMP," +
            "  orgStrId varchar(255), " +
            "  name varchar(255), " +
            "  salary int" +
            "  )");

        st.execute("create table \"" + orgCacheName + "\".Organization" +
            "  (" +
            "  _key int not null," +
            "  _val other not null," +
            "  id int unique," +
            "  strId varchar(255) ," +
            "  dateId TIMESTAMP ," +
            "  name varchar(255) " +
            "  )");

        conn.commit();

        st.close();

        for (Account account : data.accounts) {
            ignite(0).cache(accCacheName).put(account.key(useColocatedDate()), account);

            insertInDb(account);
        }

        for (Person person : data.persons) {
            ignite(0).cache(personCacheName).put(person.key(useColocatedDate()), person);

            insertInDb(person);
        }

        for (Organization org : data.orgs) {
            ignite(0).cache(orgCacheName).put(org.id, org);

            insertInDb(org);
        }
    }

    /**
     * @param acc Account.
     * @throws SQLException If failed.
     */
    private void insertInDb(Account acc) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + accCacheName + "\".Account (_key, _val, id, personId, personDateId, personStrId) " +
                "values(?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, acc.key(useColocatedDate()));
            st.setObject(++i, acc);
            st.setObject(++i, acc.id);
            st.setObject(++i, acc.personId);
            st.setObject(++i, acc.personDateId);
            st.setObject(++i, acc.personStrId);

            st.executeUpdate();
        }
    }

    /**
     * @param p Person.
     * @throws SQLException If failed.
     */
    private void insertInDb(Person p) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + personCacheName + "\".Person (_key, _val, id, strId, dateId, name, orgId, orgDateId, " +
                "orgStrId, salary) " +
                "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, p.key(useColocatedDate()));
            st.setObject(++i, p);
            st.setObject(++i, p.id);
            st.setObject(++i, p.strId);
            st.setObject(++i, p.dateId);
            st.setObject(++i, p.name);
            st.setObject(++i, p.orgId);
            st.setObject(++i, p.orgDateId);
            st.setObject(++i, p.orgStrId);
            st.setObject(++i, p.salary);

            st.executeUpdate();
        }
    }

    /**
     * @param o Organization.
     * @throws SQLException If failed.
     */
    private void insertInDb(Organization o) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + orgCacheName + "\".Organization (_key, _val, id, strId, dateId, name) " +
                "values(?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, o.id);
            st.setObject(++i, o);
            st.setObject(++i, o.id);
            st.setObject(++i, o.strId);
            st.setObject(++i, o.dateId);
            st.setObject(++i, o.name);

            st.executeUpdate();
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        compareQueryRes0(ignite(0).cache(accCacheName), "select _key, _val, id, personId, personDateId, personStrId " +
            "from \"" + accCacheName + "\".Account");

        compareQueryRes0(ignite(0).cache(personCacheName), "select _key, _val, id, strId, dateId, name, orgId, " +
            "orgDateId, orgStrId, salary from \"" + personCacheName + "\".Person");

        compareQueryRes0(ignite(0).cache(orgCacheName), "select _key, _val, id, strId, dateId, name " +
            "from \"" + orgCacheName + "\".Organization");
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();

        st.execute("CREATE SCHEMA \"" + TestCacheType.REPLICATED_1.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.REPLICATED_2.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.REPLICATED_3.cacheName + "\"");

        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b0_1.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b0_2.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b0_3.cacheName + "\"");

        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b1_1.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b1_2.cacheName + "\"");
        st.execute("CREATE SCHEMA \"" + TestCacheType.PARTITIONED_b1_3.cacheName + "\"");

        return st;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     * @return Distributed joins flag.
     */
    protected boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Use colocated data.
     */
    private boolean useColocatedDate() {
        return !distributedJoins();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoins1() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet1();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoins2() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet2();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoins3() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet3();
    }

    /**
     * @throws Exception If failed.
     */
    public void testColocatedJoins1() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet1();
    }

    /**
     * @throws Exception If failed.
     */
    public void testColocatedJoins2() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet2();
    }

    /**
     * @throws Exception If failed.
     */
    public void testColocatedJoins3() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet3();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet1() throws Exception {
        final Set<TestCacheType> personCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.REPLICATED_1);
        }};

        final Set<TestCacheType> accCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.REPLICATED_1);
            add(TestCacheType.PARTITIONED_b0_1);
            add(TestCacheType.PARTITIONED_b1_1);
            add(TestCacheType.REPLICATED_2);
            add(TestCacheType.PARTITIONED_b0_2);
            add(TestCacheType.PARTITIONED_b1_2);
        }};

        Set<TestCacheType> orgCacheTypes = new LinkedHashSet<TestCacheType>() {{
            addAll(accCacheTypes);
            add(TestCacheType.REPLICATED_3);
            add(TestCacheType.PARTITIONED_b0_3);
            add(TestCacheType.PARTITIONED_b1_3);
        }};

        checkAllCacheCombinations(accCacheTypes, personCacheTypes, orgCacheTypes);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet2() throws Exception {
        final Set<TestCacheType> personCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.PARTITIONED_b0_1);
        }};

        final Set<TestCacheType> accCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.REPLICATED_1);
            add(TestCacheType.PARTITIONED_b0_1);
            add(TestCacheType.PARTITIONED_b1_1);
            add(TestCacheType.REPLICATED_2);
            add(TestCacheType.PARTITIONED_b0_2);
            add(TestCacheType.PARTITIONED_b1_2);
        }};

        Set<TestCacheType> orgCacheTypes = new LinkedHashSet<TestCacheType>() {{
            addAll(accCacheTypes);
            add(TestCacheType.REPLICATED_3);
            add(TestCacheType.PARTITIONED_b0_3);
            add(TestCacheType.PARTITIONED_b1_3);
        }};

        checkAllCacheCombinations(accCacheTypes, personCacheTypes, orgCacheTypes);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet3() throws Exception {
        final Set<TestCacheType> personCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.PARTITIONED_b0_1);
        }};

        final Set<TestCacheType> accCacheTypes = new LinkedHashSet<TestCacheType>() {{
            add(TestCacheType.REPLICATED_1);
            add(TestCacheType.PARTITIONED_b0_1);
            add(TestCacheType.PARTITIONED_b1_1);
            add(TestCacheType.REPLICATED_2);
            add(TestCacheType.PARTITIONED_b0_2);
            add(TestCacheType.PARTITIONED_b1_2);
        }};

        Set<TestCacheType> orgCacheTypes = new LinkedHashSet<TestCacheType>() {{
            addAll(accCacheTypes);
            add(TestCacheType.REPLICATED_3);
            add(TestCacheType.PARTITIONED_b0_3);
            add(TestCacheType.PARTITIONED_b1_3);
        }};

        checkAllCacheCombinations(accCacheTypes, personCacheTypes, orgCacheTypes);
    }

    /**
     * @throws Exception If failed.
     * @param accCacheTypes Account testedCache types.
     * @param personCacheTypes Person testedCache types.
     * @param orgCacheTypes Organization testedCache types.
     */
    private void checkAllCacheCombinations(
        Set<TestCacheType> accCacheTypes, Set<TestCacheType> personCacheTypes,
        Set<TestCacheType> orgCacheTypes) throws Exception {
        Map<TestConfig, Throwable> errors = new LinkedHashMap<>();
        List<TestConfig> success = new ArrayList<>();

        int cfgIdx = 0;

        for (TestCacheType personCacheType : personCacheTypes) {
            for (TestCacheType accCacheType : accCacheTypes) {
                for (TestCacheType orgCacheType : orgCacheTypes) {
                    try {
                        check(personCacheType, accCacheType, orgCacheType);

                        success.add(new TestConfig(cfgIdx, cache, personCacheType, accCacheType, orgCacheType, qry));
                    }
                    catch (Throwable e) {
                        error("", e);

                        errors.put(new TestConfig(cfgIdx, cache, personCacheType, accCacheType, orgCacheType, qry), e);

                        // TODO delete.
                        throw new IllegalStateException(new TestConfig(cfgIdx, cache, personCacheType, accCacheType, orgCacheType, qry).toString(), e);
                    }

                    cfgIdx++;
                }
            }
        }

        if (!errors.isEmpty()) {
            int total = personCacheTypes.size() * accCacheTypes.size() * orgCacheTypes.size();

            SB sb = new SB("Test failed for the following " + errors.size() + " combination(s) (" + total + " total):\n");

            for (Map.Entry<TestConfig, Throwable> e : errors.entrySet())
                sb.a(e.getKey()).a(", error=").a(e.getValue()).a("\n");

            sb.a("Successfully finished combinations:\n");

            for (TestConfig t : success)
                sb.a(t).a("\n");

            sb.a("The following data has beed used for test:\n " + dataAsString());

            fail(sb.toString());
        }
    }

    /**
     * @param personCacheType Person testedCache personCacheType.
     * @param accCacheType Account testedCache personCacheType.
     * @param orgCacheType Organization testedCache personCacheType.
     * @throws Exception If failed.
     */
    private void check(final TestCacheType personCacheType,
        final TestCacheType accCacheType,
        final TestCacheType orgCacheType) throws Exception {
        info("Checking distributed cross testedCache join [personCache=" + personCacheType +
            ", accCache=" + accCacheType +
            ", orgCache=" + orgCacheType + "]");

        Collection<TestCacheType> cacheTypes = new ArrayList<TestCacheType>() {{
            add(personCacheType);
            add(accCacheType);
            add(orgCacheType);
        }};

        for (TestCacheType type : cacheTypes) {
            CacheConfiguration cc = cacheConfiguration(type.cacheName,
                type.cacheMode,
                type.backups,
                type == accCacheType,
                type == personCacheType,
                type == orgCacheType
            );

            ignite(0).getOrCreateCache(cc);

            info("Created testedCache [name=" + type.cacheName + ", mode=" + type.cacheMode + "]");
        }

        awaitPartitionMapExchange();

        try {
            dataAsStr = null;
            data = prepareData();

            personCacheName = personCacheType.cacheName;
            accCacheName = accCacheType.cacheName;
            orgCacheName = orgCacheType.cacheName;

            initCacheAndDbData();

            checkAllDataEquals();

            List<String> cacheNames = new ArrayList<>();

            cacheNames.add(personCacheType.cacheName);
            cacheNames.add(orgCacheType.cacheName);
            cacheNames.add(accCacheType.cacheName);

            for (int i = 0; i < NODES; i++) {
                log.info("Test node: " + i);

                for (String cacheName : cacheNames) {
                    cache = ignite(i).cache(cacheName);

                    log.info("Use testedCache: " + cache.getName());

                    if (((IgniteCacheProxy)cache).context().isReplicated() && !ignite(i).configuration().isClientMode())
                        assertProperException(cache);
                    else {
                        boolean isClientNodeAndCacheIsReplicated = ((IgniteCacheProxy)cache).context().isReplicated()
                            && ignite(i).configuration().isClientMode();

                        boolean all3CachesAreReplicated =
                            ((IgniteCacheProxy)ignite(0).cache(accCacheName)).context().isReplicated()
                                && ((IgniteCacheProxy)ignite(0).cache(personCacheName)).context().isReplicated()
                                && ((IgniteCacheProxy)ignite(0).cache(orgCacheName)).context().isReplicated();

                        // Queries running on replicated testedCache should not contain JOINs with partitioned tables.
                        if (!isClientNodeAndCacheIsReplicated || all3CachesAreReplicated) {
                            if (!cache.getName().equals(orgCacheType.cacheName))
                                checkPersonAccountsJoin(cache, data.accountsPerPerson);

                            if (!cache.getName().equals(accCacheType.cacheName))
                                checkOrganizationPersonsJoin(cache, data.personsPerOrg);

                            checkOrganizationPersonAccountJoin(cache, data.accountsPerOrg); // TODO bad query

                            checkUninon(); // TODO bad query
                            checkUninonAll(); // TODO bad query

                            if (!cache.getName().equals(orgCacheType.cacheName))
                                checkPersonAccountCrossJoin(cache);

                            if (!cache.getName().equals(accCacheType.cacheName))
                                checkPersonOrganizationGroupBy(cache);

                            if (!cache.getName().equals(orgCacheType.cacheName))
                                checkPersonAccountGroupBy(cache);

                            checkGroupBy(); // TODO bad result
                        }
                    }
                }
            }
        }
        finally {
            ignite(0).destroyCache(accCacheType.cacheName);
            ignite(0).destroyCache(personCacheType.cacheName);
            ignite(0).destroyCache(orgCacheType.cacheName);

            Statement st = conn.createStatement();

            st.execute("drop table \"" + accCacheName + "\".Account");
            st.execute("drop table \"" + personCacheName + "\".Person");
            st.execute("drop table \"" + orgCacheName + "\".Organization");

            conn.commit();

            st.close();
        }
    }

    /**
     * @param cache Cache.
     */
    private void assertProperException(final IgniteCache cache) {
        qry = "assertProperException";

        final IgniteCache accCache = ignite(0).cache(accCacheName);
        final IgniteCache personCache = ignite(0).cache(personCacheName);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.query(new SqlFieldsQuery("select p.name from " +
                    "\"" + personCache.getName() + "\".Person p, " +
                    "\"" + accCache.getName() + "\".Account a " +
                    "where p._key = a.personId").setDistributedJoins(true));

                return null;
            }
        }, CacheException.class, "Queries using distributed JOINs have to be run on partitioned cache");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.query(new SqlQuery(Person.class,
                    "from \"" + personCache.getName() + "\".Person , " +
                        "\"" + accCache.getName() + "\".Account  " +
                        "where Person._key = Account.personId")
                    .setDistributedJoins(true));

                return null;
            }
        }, CacheException.class, "Queries using distributed JOINs have to be run on partitioned cache");
    }

    /**
     * Organization ids: [0, 9]. Person ids: randoms at [10, 9999]. Accounts ids: randoms at [10000, 999_999]
     *
     * @return Data.
     */
    private static Data prepareData() {
        Map<Integer, Integer> personsPerOrg = new HashMap<>();
        Map<Integer, Integer> accountsPerPerson = new HashMap<>();
        Map<Integer, Integer> accountsPerOrg = new HashMap<>();
        Map<Integer, Integer> maxSalaryPerOrg = new HashMap<>();

        Collection<Organization> orgs = new ArrayList<>();
        Collection<Person> persons = new ArrayList<>();
        Collection<Account> accounts = new ArrayList<>();

        final int ORG_CNT = 10;

        for (int id = 0; id < ORG_CNT; id++)
            orgs.add(new Organization(id, "org-" + id));

        Set<Integer> personIds = new HashSet<>();
        Set<Integer> accountIds = new HashSet<>();

        for (int orgId = 0; orgId < ORG_CNT; orgId++) {
            int personsCnt = ThreadLocalRandom.current().nextInt(20);

            int accsPerOrg = 0;
            int maxSalary = -1;

            for (int p = 0; p < personsCnt; p++) {
                int personId = ThreadLocalRandom.current().nextInt(10, 10_000);

                while (!personIds.add(personId))
                    personId = ThreadLocalRandom.current().nextInt(10, 10_000);

                String name = "person-" + personId;

                int salary = ThreadLocalRandom.current().nextInt(1, 10) * 1000;

                if (salary > maxSalary)
                    maxSalary = salary;

                persons.add(new Person(personId, orgId, name, salary));

                int accountsCnt = ThreadLocalRandom.current().nextInt(10);

                for (int a = 0; a < accountsCnt; a++) {
                    int accountId = ThreadLocalRandom.current().nextInt(10_000, 1000_00);

                    while (!accountIds.add(accountId))
                        accountId = ThreadLocalRandom.current().nextInt(10_000, 1000_000);

                    accounts.add(new Account(accountId, personId, orgId));
                }

                accountsPerPerson.put(personId, accountsCnt);

                accsPerOrg += accountsCnt;
            }

            personsPerOrg.put(orgId, personsCnt);
            accountsPerOrg.put(orgId, accsPerOrg);
            maxSalaryPerOrg.put(orgId, maxSalary);
        }

        return new Data(orgs, persons, accounts, personsPerOrg, accountsPerPerson, accountsPerOrg, maxSalaryPerOrg);
    }

    /**
     * @param cacheName Cache name.
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param accountIdx Account index flag.
     * @param personIdx Person index flag.
     * @param orgIdx Organization index flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName,
        CacheMode cacheMode,
        int backups,
        boolean accountIdx,
        boolean personIdx,
        boolean orgIdx) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(cacheName);

        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        List<QueryEntity> entities = new ArrayList<>();

        if (accountIdx) {
            QueryEntity account = new QueryEntity();
            account.setKeyType(useColocatedDate() ? AffinityKey.class.getName() : Integer.class.getName());
            account.setValueType(Account.class.getName());
            account.addQueryField("id", Integer.class.getName(), null);
            account.addQueryField("personId", Integer.class.getName(), null);
            account.addQueryField("personDateId", Date.class.getName(), null);
            account.addQueryField("personStrId", String.class.getName(), null);

            entities.add(account);
        }

        if (personIdx) {
            QueryEntity person = new QueryEntity();
            person.setKeyType(useColocatedDate() ? AffinityKey.class.getName() : Integer.class.getName());
            person.setValueType(Person.class.getName());
            person.addQueryField("id", Integer.class.getName(), null);
            person.addQueryField("dateId", Date.class.getName(), null);
            person.addQueryField("strId", String.class.getName(), null);
            person.addQueryField("orgId", Integer.class.getName(), null);
            person.addQueryField("orgDateId", Date.class.getName(), null);
            person.addQueryField("orgStrId", String.class.getName(), null);
            person.addQueryField("name", String.class.getName(), null);
            person.addQueryField("salary", Integer.class.getName(), null);

            entities.add(person);
        }

        if (orgIdx) {
            QueryEntity org = new QueryEntity();
            org.setKeyType(Integer.class.getName());
            org.setValueType(Organization.class.getName());
            org.addQueryField("id", Integer.class.getName(), null);
            org.addQueryField("dateId", Date.class.getName(), null);
            org.addQueryField("strId", String.class.getName(), null);
            org.addQueryField("name", String.class.getName(), null);

            entities.add(org);
        }

        ccfg.setQueryEntities(entities);

        return ccfg;
    }

    /**
     * @param cache Cache.
     * @param cnts Organizations per person counts.
     */
    private void checkOrganizationPersonsJoin(IgniteCache cache, Map<Integer, Integer> cnts) {
        qry = "checkOrganizationPersonsJoin";

        SqlFieldsQuery qry = new SqlFieldsQuery("select o.name, p.name " +
            "from \"" + orgCacheName + "\".Organization o, \"" + personCacheName + "\".Person p " +
            "where p.orgId = o._key and o._key=?");

        qry.setDistributedJoins(true);

        SqlQuery qry2 = null;

        if (personCacheName.equals(cache.getName())) {
            qry2 = new SqlQuery(Person.class,
                "from \"" + orgCacheName + "\".Organization, \"" + personCacheName + "\".Person " +
                    "where Person.orgId = Organization._key and Organization._key=?"
            );

            qry2.setDistributedJoins(true);
        }

        long total = 0;

        for (int i = 0; i < cnts.size(); i++) {
            qry.setArgs(i);

            if (qry2 != null)
                qry2.setArgs(i);

            List<List<Object>> res = cache.query(qry).getAll();

            assertEquals((int)cnts.get(i), res.size());

            if (qry2 != null) {
                List<List<Object>> res2 = cache.query(qry2).getAll();

                assertEquals((int)cnts.get(i), res2.size());
            }

            total += res.size();
        }

        SqlFieldsQuery qry3 = new SqlFieldsQuery("select count(*) " +
            "from \"" + orgCacheName + "\".Organization o, \"" + personCacheName + "\".Person p where p.orgId = o._key");

        qry3.setDistributedJoins(true);

        List<List<Object>> res = cache.query(qry3).getAll();

        assertEquals(1, res.size());
        assertEquals(total, res.get(0).get(0));
    }

    /**
     * @param cache Cache.
     * @param cnts Accounts per person counts.
     */
    private void checkPersonAccountsJoin(IgniteCache cache, Map<Integer, Integer> cnts) {
        qry = "checkPersonAccountsJoin";

        List<Query> qrys = new ArrayList<>();

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p._key = a.personId and p._key=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p.dateId = a.personDateId and p._key=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p.strId = a.personStrId and p._key=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p.id = a.personId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p.dateId = a.personDateId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + personCacheName + "\".Person p, " +
                "\"" + accCacheName + "\".Account a " +
                "where p.strId = a.personStrId and p.id=?")
        );

        if (personCacheName.equals(cache.getName())) {
            qrys.add(new SqlQuery(Person.class,
                    "from \"" + personCacheName + "\".Person , \"" + accCacheName + "\".Account  " +
                        "where Person._key = Account.personId and Person._key=?")
            );

            qrys.add(new SqlQuery(Person.class,
                    "from \"" + personCacheName + "\".Person , \"" + accCacheName + "\".Account  " +
                        "where Person.id = Account.personId and Person.id=?")
            );
        }

        long total = 0;

        for (Map.Entry<Integer, Integer> e : cnts.entrySet()) {
            List<List<Object>> res = null;

            for (Query q : qrys) {
                if (q instanceof SqlFieldsQuery) {
                    ((SqlFieldsQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlFieldsQuery)q).setArgs(e.getKey());
                }
                else {
                    ((SqlQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlQuery)q).setArgs(e.getKey());
                }

                res = cache.query(q).getAll();

                assertEquals((int)e.getValue(), res.size());
            }

            total += res.size();
        }

        qrys.clear();

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + personCacheName + "\".Person p, \"" + accCacheName + "\".Account" + " a " +
            "where p.id = a.personId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + personCacheName + "\".Person p, \"" + accCacheName + "\".Account" + " a " +
            "where p.Dateid = a.personDateId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + personCacheName + "\".Person p, \"" + accCacheName + "\".Account" + " a " +
            "where p.strId = a.personStrId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + personCacheName + "\".Person p, \"" + accCacheName + "\".Account" + " a " +
            "where p._key = a.personId"));

        for (Query q : qrys) {
            ((SqlFieldsQuery)q).setDistributedJoins(distributedJoins());

            List<List<Object>> res = cache.query(q).getAll();

            assertEquals(1, res.size());
            assertEquals(total, res.get(0).get(0));
        }
    }

    /**
     * @param cache Cache.
     * @param cnts Accounts per organization count
     */
    private void checkOrganizationPersonAccountJoin(IgniteCache cache, Map<Integer, Integer> cnts) {
        qry = "checkOrganizationPersonAccountJoin";

        List<Query> queries = new ArrayList<>();

        queries.add(new SqlFieldsQuery("select o.name, p.name, a._key " +
            "from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p.orgId = o._key and p._key = a.personId and o.id = ?"));

        queries.add(new SqlFieldsQuery("select o.name, p.name, a._key " +
            "from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p.orgDateId = o.dateId and p.strId = a.personStrId and o.id = ?"));

        queries.add(new SqlFieldsQuery("select o.name, p.name, a._key " +
            "from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p.orgStrId = o.strId and p.id = a.personId and o.id = ?"));

        if (accCacheName.equals(cache.getName())) {
            queries.add(new SqlQuery(Account.class, "from " +
                "\"" + orgCacheName + "\".Organization , " +
                "\"" + personCacheName + "\".Person , " +
                "\"" + accCacheName + "\".Account  " +
                "where Person.orgId = Organization._key and Person._key = Account.personId and Organization.id = ?"));
        }

        long total = 0;

        for (int orgId = 0; orgId < cnts.size(); orgId++) {
            List<List<Object>> res = null;

            for (Query q : queries) {
                if (q instanceof SqlFieldsQuery) {
                    ((SqlFieldsQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlFieldsQuery)q).setArgs(orgId);
                }
                else {
                    ((SqlQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlQuery)q).setArgs(orgId);
                }

                res = cache.query(q).getAll();

                assertEquals((int)cnts.get(orgId), res.size());
            }

            total += res.size();
        }

        SqlFieldsQuery qry3 = new SqlFieldsQuery("select count(*) " +
            "from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p.orgId = o._key and p._key = a.personId");

        qry3.setDistributedJoins(distributedJoins());

        List<List<Object>> res = cache.query(qry3).getAll();

        assertEquals(1, res.size());
        assertEquals(total, res.get(0).get(0));

    }

    /**
     *
     */
    private void checkUninonAll() throws SQLException {
        qry = "checkUninonAll";

        String sql = "select p.name from " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p._key = a.personId and p.id = ? " +
            "union all " +
            "select o.name from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p " +
            "where p.orgStrId = o.strId and o.id = ?";

        for (Person person : data.persons) {
            for (Organization org : data.orgs)
                compareQueryRes0(cache, sql, distributedJoins(), new Object[] {person.id, org.id}, Ordering.RANDOM);
        }
    }

    /**
     *
     */
    private void checkUninon() throws SQLException {
        qry = "checkUninonAll";

        String sql = "select p.name from " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + accCacheName + "\".Account a " +
            "where p._key = a.personId and p.id = ? " +
            "union " +
            "select o.name from " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + personCacheName + "\".Person p " +
            "where p.orgStrId = o.strId and o.id = ?";

        for (Person person : data.persons) {
            for (Organization org : data.orgs)
                compareQueryRes0(cache, sql, distributedJoins(), new Object[] {person.id, org.id}, Ordering.RANDOM);
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonAccountCrossJoin(IgniteCache cache) {
        qry = "checkPersonAccountCrossJoin";

        SqlFieldsQuery q = new SqlFieldsQuery("select p.name " +
            "from \"" + personCacheName + "\".Person p " +
            "cross join \"" + accCacheName + "\".Account a");

        q.setDistributedJoins(distributedJoins());

        List res = cache.query(q).getAll();

        assertEquals(data.persons.size() * data.accounts.size(), res.size());
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonOrganizationGroupBy(IgniteCache cache) {
        qry = "checkPersonOrganizationGroupBy";

        // Max salary per organization.
        SqlFieldsQuery q = new SqlFieldsQuery("select max(p.salary) " +
            "from \"" + personCacheName + "\".Person p join \"" + orgCacheName + "\".Organization o " +
            "on p.orgId = o.id " +
            "group by o.name " +
            "having o.id = ?");

        q.setDistributedJoins(distributedJoins());

        for (Map.Entry<Integer, Integer> e : data.maxSalaryPerOrg.entrySet()) {
            Integer orgId = e.getKey();
            Integer maxSalary = e.getValue();

            q.setArgs(orgId);

            List<List<?>> res = cache.query(q).getAll();

            String errMsg = "Expected data [orgId=" + orgId + ", maxSalary=" + maxSalary + ", data=" + dataAsString() + "]";

            // MaxSalary == -1 means that there are no persons at organization.
            if (maxSalary > 0) {
                assertEquals(errMsg, 1, res.size());
                assertEquals(errMsg, 1, res.get(0).size());
                assertEquals(errMsg, maxSalary, res.get(0).get(0));
            }
            else
                assertEquals(errMsg, 0, res.size());
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonAccountGroupBy(IgniteCache cache) {
        qry = "checkPersonAccountGroupBy";

        // Count accounts per person.
        SqlFieldsQuery q = new SqlFieldsQuery("select count(a.id) " +
            "from \"" + personCacheName + "\".Person p join \"" + accCacheName + "\".Account a " +
            "on p.strId = a.personStrId " +
            "group by p.name " +
            "having p.id = ?");

        q.setDistributedJoins(distributedJoins());

        for (Map.Entry<Integer, Integer> e : data.accountsPerPerson.entrySet()) {
            Integer personId = e.getKey();
            Integer cnt = e.getValue();

            q.setArgs(personId);

            List<List<?>> res = cache.query(q).getAll();

            String errMsg = "Expected data [personId=" + personId + ", cnt=" + cnt + ", data=" + dataAsString() + "]";

            // Cnt == 0 means that there are no accounts for the person.
            if (cnt > 0) {
                assertEquals(errMsg, 1, res.size());
                assertEquals(errMsg, 1, res.get(0).size());
                assertEquals(errMsg, (long)cnt, res.get(0).get(0));
            }
            else
                assertEquals(errMsg, 0, res.size());
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonAccountOrganizationGroupBy(IgniteCache cache) {
        qry = "checkPersonAccountOrganizationGroupBy";

        // Max count of accounts at org.
        SqlFieldsQuery q = new SqlFieldsQuery("select max(count(a.id)) " +
            "from " +
            "\"" + personCacheName + "\".Person p " +
            "\"" + orgCacheName + "\".Organization o " +
            "\"" + accCacheName + "\".Account a " +
            "where p.id = a.personId and p.orgStrId = o.strId " +
            "group by org.id " +
            "having o.id = ?");

        q.setDistributedJoins(distributedJoins());

        for (Map.Entry<Integer, Integer> e : data.accountsPerPerson.entrySet()) {
            Integer personId = e.getKey();
            Integer cnt = e.getValue();

            q.setArgs(personId);

            List<List<?>> res = cache.query(q).getAll();

            String errMsg = "Expected data [personId=" + personId + ", cnt=" + cnt + ", data=" + dataAsString() + "]";

            // Cnt == 0 means that there are no accounts for the person.
            if (cnt > 0) {
                assertEquals(errMsg, 1, res.size());
                assertEquals(errMsg, 1, res.get(0).size());
                assertEquals(errMsg, (long)cnt, res.get(0).get(0));
            }
            else
                assertEquals(errMsg, 0, res.size());
        }
    }

    /**
     *
     */
    private void checkGroupBy() throws SQLException {
        qry = "checkGroupBy";

        // Select persons with count of accounts of person at organization.
        String sql = "select p.id, count(a.id) " +
            "from " +
            "\"" + personCacheName + "\".Person p, " +
            "\"" + orgCacheName + "\".Organization o, " +
            "\"" + accCacheName + "\".Account a " +
            "where p.id = a.personId and p.orgStrId = o.strId " +
            "group by p.id " +
            "having o.id = ?";

        for (Organization org : data.orgs)
            compareQueryRes0(cache, sql, distributedJoins(), new Object[] {org.id}, Ordering.RANDOM);
    }

    /**
     * @return Data as string.
     */
    private String dataAsString() {
        if (dataAsStr == null)
            dataAsStr = data.toString();

        return dataAsStr;
    }

    /**
     *
     */
    private enum TestCacheType {
        /** */
        REPLICATED_1(CacheMode.REPLICATED, 0),

        /** */
        REPLICATED_2(CacheMode.REPLICATED, 0),

        /** */
        REPLICATED_3(CacheMode.REPLICATED, 0),

        /** */
        PARTITIONED_b0_1(CacheMode.PARTITIONED, 0),

        /** */
        PARTITIONED_b0_2(CacheMode.PARTITIONED, 0),

        /** */
        PARTITIONED_b0_3(CacheMode.PARTITIONED, 0),

        /** */
        PARTITIONED_b1_1(CacheMode.PARTITIONED, 1),

        /** */
        PARTITIONED_b1_2(CacheMode.PARTITIONED, 1),

        /** */
        PARTITIONED_b1_3(CacheMode.PARTITIONED, 1);

        /** */
        final String cacheName;

        /** */
        final CacheMode cacheMode;

        /** */
        final int backups;

        /**
         * @param mode Cache mode.
         * @param backups Backups.
         */
        TestCacheType(CacheMode mode, int backups) {
            cacheName = name();
            cacheMode = mode;
            this.backups = backups;
        }
    }

    /**
     *
     */
    private static class Data {
        /** */
        final Collection<Organization> orgs;

        /** */
        final Collection<Person> persons;

        /** */
        final Collection<Account> accounts;

        /** */
        final Map<Integer, Integer> personsPerOrg;

        /** PersonId to count of accounts that person has. */
        final Map<Integer, Integer> accountsPerPerson;

        /** */
        final Map<Integer, Integer> accountsPerOrg;

        /** */
        final Map<Integer, Integer> maxSalaryPerOrg;

        /**
         * @param orgs Organizations.
         * @param persons Persons.
         * @param accounts Accounts.
         * @param personsPerOrg Count of persons per organization.
         * @param accountsPerPerson Count of accounts per person.
         * @param accountsPerOrg Count of accounts per organization.
         */
        Data(Collection<Organization> orgs, Collection<Person> persons, Collection<Account> accounts,
            Map<Integer, Integer> personsPerOrg, Map<Integer, Integer> accountsPerPerson,
            Map<Integer, Integer> accountsPerOrg, Map<Integer, Integer> maxSalaryPerOrg) {
            this.orgs = orgs;
            this.persons = persons;
            this.accounts = accounts;
            this.personsPerOrg = personsPerOrg;
            this.accountsPerPerson = accountsPerPerson;
            this.accountsPerOrg = accountsPerOrg;
            this.maxSalaryPerOrg = maxSalaryPerOrg;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Data{" +
                "orgs=" + orgs +
                ", persons=" + persons +
                ", accounts=" + accounts +
                ", personsPerOrg=" + personsPerOrg +
                ", accountsPerPerson=" + accountsPerPerson +
                ", accountsPerOrg=" + accountsPerOrg +
                ", maxSalaryPerOrg=" + maxSalaryPerOrg +
                '}';
        }
    }

    /**
     *
     */
    private static class Account implements Serializable {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private int personId;

        /** */
        @QuerySqlField
        private Date personDateId;

        /** */
        @QuerySqlField
        private String personStrId;

        @QuerySqlField
        private int orgId;

        /**
         * @param id ID.
         * @param personId Person ID.
         */
        Account(int id, int personId, int orgId) {
            this.id = id;
            this.personId = personId;
            this.orgId = orgId;
            personDateId = new Date(personId);
            personStrId = "personId" + personId;
        }

        /**
         * @param useColocatedData Use colocated data.
         * @return Key.
         */
        public Object key(boolean useColocatedData) {
            return useColocatedData ? new AffinityKey<>(id, orgId) : id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account{" +
                "id=" + id +
                ", personId=" + personId +
                '}';
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        int id;

        /** Date as ID. */
        @QuerySqlField
        Date dateId;

        /** String as ID */
        @QuerySqlField
        String strId;

        /** */
        @QuerySqlField
        int orgId;

        /** */
        @QuerySqlField
        Date orgDateId;

        /** */
        @QuerySqlField
        String orgStrId;

        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        int salary;

        /**
         * @param id ID.
         * @param orgId Organization ID.
         * @param name Name.
         */
        Person(int id, int orgId, String name, int salary) {
            this.id = id;
            dateId = new Date(id);
            strId = "personId" + id;
            this.orgId = orgId;
            orgDateId = new Date(orgId);
            orgStrId = "orgId" + orgId;
            this.name = name;
            this.salary = salary;
        }

        /**
         * @param useColocatedData Use colocated data.
         * @return Key.
         */
        public Object key(boolean useColocatedData) {
            return useColocatedData ? new AffinityKey<>(id, orgId) : id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person{" +
                "id=" + id +
                ", orgId=" + orgId +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                '}';
        }
    }

    /**
     *
     */
    private static class Organization implements Serializable {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        Date dateId;

        /** */
        @QuerySqlField
        String strId;

        /** */
        @QuerySqlField
        String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Organization(int id, String name) {
            this.id = id;
            dateId = new Date(id);
            strId = "orgId" + id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
        }
    }

    /**
     *
     */
    private static class TestConfig {
        /** */
        private final int idx;

        /** */
        private final IgniteCache testedCache;

        /** */
        private final TestCacheType personCacheType;

        /** */
        private final TestCacheType accCacheType;

        /** */
        private final TestCacheType orgCacheType;

        /** */
        private final String qry;

        /**
         * @param cfgIdx Tested configuration index.
         * @param testedCache Tested testedCache.
         * @param personCacheType Person testedCache personCacheType.
         * @param accCacheType Account testedCache personCacheType.
         * @param orgCacheType Organization testedCache personCacheType.
         * @param testedQry Query.
         */
        TestConfig(int cfgIdx, IgniteCache testedCache, TestCacheType personCacheType,
            TestCacheType accCacheType, TestCacheType orgCacheType, String testedQry) {

            idx = cfgIdx;
            this.testedCache = testedCache;
            this.personCacheType = personCacheType;
            this.accCacheType = accCacheType;
            this.orgCacheType = orgCacheType;
            qry = testedQry;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestConfig{" +
                "idx=" + idx +
                ", testedCache=" + testedCache.getName() +
                ", personCacheType=" + personCacheType +
                ", accCacheType=" + accCacheType +
                ", orgCacheType=" + orgCacheType +
                ", qry=" + qry +
                '}';
        }
    }
}
