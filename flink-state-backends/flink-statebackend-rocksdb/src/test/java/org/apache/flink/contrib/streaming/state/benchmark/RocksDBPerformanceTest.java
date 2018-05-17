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

package org.apache.flink.contrib.streaming.state.benchmark;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.CompactionStyle;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import sun.misc.Unsafe;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Test that validates that the performance of RocksDB is as expected.
 * This test guards against the bug filed as 'FLINK-5756'
 */
public class RocksDBPerformanceTest extends TestLogger {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final RetryRule retry = new RetryRule();

	private File rocksDir;
	private Options options;
	private WriteOptions writeOptions;

	private final String key = "key";
	private final String value = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ7890654321";

	private final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
	private final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

	@Before
	public void init() throws IOException {
		rocksDir = tmp.newFolder();

		// ensure the RocksDB library is loaded to a distinct location each retry
		NativeLibraryLoader.getInstance().loadLibrary(rocksDir.getAbsolutePath());

		options = new Options()
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevelCompactionDynamicLevelBytes(true)
				.setIncreaseParallelism(4)
				.setUseFsync(false)
				.setMaxOpenFiles(-1)
				.setCreateIfMissing(true)
				.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);

		writeOptions = new WriteOptions()
				.setSync(false)
				.setDisableWAL(true);
	}

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void testRocksDbMergePerformance() throws Exception {
		final int num = 50000;

		try (RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath())) {

			// ----- insert -----
			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				rocksDB.merge(writeOptions, keyBytes, valueBytes);
			}
			final long endInsert = System.nanoTime();
			log.info("end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);

			// ----- read (attempt 1) -----

			final byte[] resultHolder = new byte[num * (valueBytes.length + 2)];
			final long beginGet1 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet1 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet1 - beginGet1) / 1_000_000);

			// ----- read (attempt 2) -----

			final long beginGet2 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet2 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet2 - beginGet2) / 1_000_000);

			// ----- compact -----
			log.info("compacting...");
			final long beginCompact = System.nanoTime();
			rocksDB.compactRange();
			final long endCompact = System.nanoTime();

			log.info("end compaction - duration: {} ms", (endCompact - beginCompact) / 1_000_000);

			// ----- read (attempt 3) -----

			final long beginGet3 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet3 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet3 - beginGet3) / 1_000_000);
		}
	}

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void testRocksDbRangeGetPerformance() throws Exception {
		final int num = 50000;

		try (RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath())) {

			final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

			final Unsafe unsafe = MemoryUtils.UNSAFE;
			final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				unsafe.putInt(keyTemplate, offset, i);
				rocksDB.put(writeOptions, keyTemplate, valueBytes);
			}
			final long endInsert = System.nanoTime();
			log.info("end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);

			@SuppressWarnings("MismatchedReadAndWriteOfArray")
			final byte[] resultHolder = new byte[num * valueBytes.length];

			final long beginGet = System.nanoTime();

			int pos = 0;

			try (final RocksIterator iterator = rocksDB.newIterator()) {
				// seek to start
				unsafe.putInt(keyTemplate, offset, 0);
				iterator.seek(keyTemplate);

				// iterate
				while (iterator.isValid() && samePrefix(keyBytes, iterator.key())) {
					byte[] currValue = iterator.value();
					System.arraycopy(currValue, 0, resultHolder, pos, currValue.length);
					pos += currValue.length;
					iterator.next();
				}
			}

			final long endGet = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet - beginGet) / 1_000_000);
		}
	}

	@Test
	public void testWriteBatchVSPut() throws Exception {

		System.out.println("---------> Batch VS Put <------------");

		File rocksDirForBatch = tmp.newFolder();
		File rocksDirForPut = tmp.newFolder();

		final int num = 50000;

		long batchCost;
		long putCost;

		try (RocksDB rocksDB = RocksDB.open(options, rocksDirForBatch.getAbsolutePath());
			 WriteBatch writeBatch = new WriteBatch()) {

			final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

			final Unsafe unsafe = MemoryUtils.UNSAFE;
			final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				unsafe.putInt(keyTemplate, offset, i);
				writeBatch.put(keyTemplate, valueBytes);
				if (i % 500 == 0) {
					rocksDB.write(writeOptions, writeBatch);
					writeBatch.clear();
				}
			}
			rocksDB.write(writeOptions, writeBatch);
			final long endInsert = System.nanoTime();
			batchCost = endInsert - beginInsert;
			log.info("BATCH: end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);
			System.out.println("BATCH: end insert - duration:" + (endInsert - beginInsert) / 1_000_000);
		}

		try (RocksDB rocksDB = RocksDB.open(options, rocksDirForPut.getAbsolutePath())) {

			final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

			final Unsafe unsafe = MemoryUtils.UNSAFE;
			final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				unsafe.putInt(keyTemplate, offset, i);
				rocksDB.put(writeOptions, keyTemplate, valueBytes);
			}
			final long endInsert = System.nanoTime();
			putCost = endInsert - beginInsert;
			log.info("PUT: end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);
			System.out.println("PUT: end insert - duration:" + (endInsert - beginInsert) / 1_000_000);
		}

		Assert.assertTrue(batchCost < putCost);
	}

	@Test
	public void testMapStateClearNewVSOld() throws Exception {

		System.out.println("---------> MapState#Clear New VS Old <------------");

		// the first comparison maybe not accuracy because of the setup reason.
		long oldCost = testMapStateClearNewVSOld(50, false);
		long newCost = testMapStateClearNewVSOld(50, true);

		System.out.println("---->");
		System.out.println("NEW: end delete " + 50 + " records - duration:" + newCost);
		System.out.println("OLD: end delete " + 50 + " records - duration:" + oldCost);

		oldCost = testMapStateClearNewVSOld(100, false);
		newCost = testMapStateClearNewVSOld(100, true);

		System.out.println("---->");
		System.out.println("NEW: end delete " + 100 + " records - duration:" + newCost);
		System.out.println("OLD: end delete " + 100 + " records - duration:" + oldCost);

		oldCost = testMapStateClearNewVSOld(200, false);
		newCost = testMapStateClearNewVSOld(200, true);

		System.out.println("---->");
		System.out.println("NEW: end delete " + 200 + " records - duration:" + newCost);
		System.out.println("OLD: end delete " + 200 + " records - duration:" + oldCost);

		oldCost = testMapStateClearNewVSOld(400, false);
		newCost = testMapStateClearNewVSOld(400, true);

		System.out.println("---->");
		System.out.println("NEW: end delete " + 400 + " records - duration:" + newCost);
		System.out.println("OLD: end delete " + 400 + " records - duration:" + oldCost);

		oldCost = testMapStateClearNewVSOld(800, false);
		newCost = testMapStateClearNewVSOld(800, true);

		System.out.println("---->");
		System.out.println("NEW: end delete " + 800 + " records - duration:" + newCost);
		System.out.println("OLD: end delete " + 800 + " records - duration:" + oldCost);
	}

	public long testMapStateClearNewVSOld(int num, boolean isNew) throws Exception {

		File rocksDirForBatch = tmp.newFolder();

		try (RocksDB rocksDB = RocksDB.open(options, rocksDirForBatch.getAbsolutePath())) {

			final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

			final Unsafe unsafe = MemoryUtils.UNSAFE;
			final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

			for (int i = 0; i < num; i++) {
				unsafe.putInt(keyTemplate, offset, i);
				rocksDB.put(keyTemplate, valueBytes);
			}

			final long beginInsert = System.nanoTime();
			if (isNew) {
				try (RocksIterator iterator = rocksDB.newIterator();
					 WriteBatch writeBatch = new WriteBatch()) {
					iterator.seek(keyBytes);
					while (iterator.isValid()) {
						if (samePrefix(keyBytes, iterator.key())) {
							writeBatch.remove(iterator.key());
						} else {
							break;
						}
						iterator.next();
					}
					rocksDB.write(writeOptions, writeBatch);
				}
			} else {
				RocksDBMapIterator iterator = new RocksDBMapIterator(rocksDB, keyBytes);
				while (iterator.hasNext()) {
					RocksDBMapEntry entry = iterator.next();
					entry.remove();
				}
			}

			return System.nanoTime() - beginInsert;
		}
	}


	class RocksDBMapEntry implements Map.Entry<byte[], byte[]> {
		private final RocksDB db;

		/** The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB
		 * with the format #KeyGroup#Key#Namespace#UserKey. */
		private final byte[] rawKeyBytes;

		/** The raw bytes of the value stored in RocksDB. */
		private byte[] rawValueBytes;

		private boolean deleted;

		RocksDBMapEntry(
			@Nonnull final RocksDB db,
			@Nonnull final byte[] rawKeyBytes,
			@Nonnull final byte[] rawValueBytes) {
			this.db = db;

			this.rawKeyBytes = rawKeyBytes;
			this.rawValueBytes = rawValueBytes;
		}

		public void remove() {
			rawValueBytes = null;

			try {
				db.delete(writeOptions, rawKeyBytes);
				deleted = true;
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while removing data from RocksDB.", e);
			}
		}

		@Override
		public byte[] getKey() {
			return null;
		}

		@Override
		public byte[] getValue() {
			return null;
		}

		@Override
		public byte[] setValue(byte[] value) {
			return null;
		}
	}

	public class RocksDBMapIterator implements Iterator<RocksDBMapEntry> {

		private static final int CACHE_SIZE_LIMIT = 128;

		private final RocksDB db;

		private final byte[] keyPrefixBytes;

		private boolean expired = false;

		private ArrayList<RocksDBMapEntry> cacheEntries = new ArrayList<>();

		private int cacheIndex = 0;

		RocksDBMapIterator(
			final RocksDB db,
			final byte[] keyPrefixBytes) {

			this.db = db;
			this.keyPrefixBytes = keyPrefixBytes;
		}

		@Override
		public boolean hasNext() {
			loadCache();

			return (cacheIndex < cacheEntries.size());
		}

		@Override
		public RocksDBMapEntry next() {
			loadCache();

			if (cacheIndex == cacheEntries.size()) {
				if (!expired) {
					throw new IllegalStateException();
				}

				return null;
			}

			RocksDBMapEntry entry = cacheEntries.get(cacheIndex);
			cacheIndex++;

			return entry;
		}

		@Override
		public void remove() {
		}

		private void loadCache() {
			if (cacheIndex > cacheEntries.size()) {
				throw new IllegalStateException();
			}

			// Load cache entries only when the cache is empty and there still exist unread entries
			if (cacheIndex < cacheEntries.size() || expired) {
				return;
			}

			// use try-with-resources to ensure RocksIterator can be release even some runtime exception
			// occurred in the below code block.
			try (RocksIterator iterator = db.newIterator()) {

				/*
				 * The iteration starts from the prefix bytes at the first loading. The cache then is
				 * reloaded when the next entry to return is the last one in the cache. At that time,
				 * we will start the iterating from the last returned entry.
 				 */
				RocksDBMapEntry lastEntry = cacheEntries.size() == 0 ? null : cacheEntries.get(cacheEntries.size() - 1);
				byte[] startBytes = (lastEntry == null ? keyPrefixBytes : lastEntry.rawKeyBytes);

				cacheEntries.clear();
				cacheIndex = 0;

				iterator.seek(startBytes);

				/*
				 * If the last returned entry is not deleted, it will be the first entry in the
				 * iterating. Skip it to avoid redundant access in such cases.
				 */
				if (lastEntry != null && !lastEntry.deleted) {
					iterator.next();
				}

				while (true) {
					if (!iterator.isValid() || !samePrefix(keyPrefixBytes, iterator.key())) {
						expired = true;
						break;
					}

					if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
						break;
					}

					RocksDBMapEntry entry = new RocksDBMapEntry(
						db,
						iterator.key(),
						iterator.value());

					cacheEntries.add(entry);

					iterator.next();
				}
			}
		}
	}

	private static boolean samePrefix(byte[] prefix, byte[] key) {
		for (int i = 0; i < prefix.length; i++) {
			if (prefix[i] != key [i]) {
				return false;
			}
		}

		return true;
	}

	@After
	public void close() {
		options.close();
		writeOptions.close();
	}
}
