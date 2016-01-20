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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryLowerCaseIdMapper;
import org.apache.ignite.binary.BinarySimpleNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Binary builder test.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class BinaryObjectBuilderSimpleNameLowerCaseMappersSelfTest extends BinaryObjectBuilderDefaultMappersSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        bCfg.setIdMapper(new BinaryLowerCaseIdMapper());
        bCfg.setNameMapper(new BinarySimpleNameMapper());

        return cfg;
    }
}