/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDB;

import java.util.Collections;

/**
 * Tests to guard {@link RocksDBFullCheckpointDataIterable}.
 */
public class RocksDBFullCheckpointDataIterableTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void basicTest() throws Exception {

		RocksDB rocksDB = RocksDB.open(tempFolder.newFolder().getAbsolutePath());

		RocksDBFullCheckpointDataIterable iterable = new RocksDBFullCheckpointDataIterable(
			null,
			new KeyGroupRange(0, 1),
			null,
			Collections.EMPTY_LIST,
			new UncompressedStreamCompressionDecorator());
	}


}
