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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.CloseableIterable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

/**
 * Tests to guard {@link SstFileBuilder}.
 */
public class SstFileBuilderTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSstFileWriterWrapper() throws Exception {

		try (final RocksDB rocksDB = RocksDB.open(tempFolder.newFolder().getAbsolutePath());
			 final ColumnFamilyHandle testHandle = rocksDB.createColumnFamily(
				 new ColumnFamilyDescriptor("test".getBytes()))) {

			SstFileBuilder.SstFileWriterWrapper sstFileWriterWrapper = new SstFileBuilder.SstFileWriterWrapper(
				tempFolder.newFolder().getAbsolutePath(),
				1024L, // 1 K
				testHandle,
				new SimpleSstHandle(rocksDB));

			// data must be inserted into sst writer asc
			int totalNumber = 1_000;
			for (int i = 0; i < totalNumber; ++i) {
				sstFileWriterWrapper.addRecord(intToBytes(i), String.format("the value for %d-th key", i).getBytes());
			}

			sstFileWriterWrapper.finish();

			// valid
			for (int i = 0; i < totalNumber; ++i) {
				Assert.assertArrayEquals(String.format("the value for %d-th key", i).getBytes(), rocksDB.get(testHandle, intToBytes(i)));
			}
		}
	}

	@Test
	public void testBuilding() throws Exception {

		ExecutorService executorService = Executors.newFixedThreadPool(4);

		try (final RocksDB rocksDB = RocksDB.open(tempFolder.newFolder().getAbsolutePath());
			 final ColumnFamilyHandle testHandle1 = rocksDB.createColumnFamily(
				 new ColumnFamilyDescriptor("test1".getBytes()));
			 final ColumnFamilyHandle testHandle2 = rocksDB.createColumnFamily(
				 new ColumnFamilyDescriptor("test2".getBytes()));
			 final ColumnFamilyHandle testHandle3 = rocksDB.createColumnFamily(
				 new ColumnFamilyDescriptor("test3".getBytes()))) {

			final ColumnFamilyHandle[] columnFamilyHandles = new ColumnFamilyHandle[3];
			columnFamilyHandles[0] = testHandle1;
			columnFamilyHandles[1] = testHandle2;
			columnFamilyHandles[2] = testHandle3;

			// building with parallelism 4
			CompletableFuture[] completableFutures = new CompletableFuture[4];
			SimpleKeyGroupIterable[] keyGroupIterables = new SimpleKeyGroupIterable[4];
			int beginNumber = 0;
			int endNumber = 10000;
			for (int i = 0; i < 4; ++i) {
				SstFileBuilder sstFileBuilder = new SstFileBuilder(
					tempFolder.newFolder().getAbsolutePath(),
					1024,
					executorService,
					new SimpleSstHandle(rocksDB));

				keyGroupIterables[i] = new SimpleKeyGroupIterable(columnFamilyHandles, beginNumber, endNumber);
				completableFutures[i] = sstFileBuilder.building(keyGroupIterables[i]);
				beginNumber = endNumber;
				endNumber += 10000;
			}

			// waiting for completed
			for (int i = 0; i < 4; ++i)	{
				completableFutures[i].get();
			}

			// valid
			for (int i = 0; i < 4; ++i) {
				SimpleKeyGroupIterable keyGroupIterable = keyGroupIterables[i];
				for (Tuple3<ColumnFamilyHandle, byte[], byte[]> record : keyGroupIterable.getRecords()) {
					Assert.assertArrayEquals(record.f2, rocksDB.get(record.f0, record.f1));
				}
			}
		} finally {
			executorService.shutdown();
		}
	}

	@Test
	@Ignore
	public void benchMark() throws Exception {

		ExecutorService executorService = Executors.newFixedThreadPool(4);

		try (final RocksDB rocksDB1 = RocksDB.open(
			PredefinedOptions.SPINNING_DISK_OPTIMIZED.createDBOptions().setCreateIfMissing(true),
			tempFolder.newFolder().getAbsolutePath(),
			Collections.singletonList(new ColumnFamilyDescriptor("default".getBytes())),
			new ArrayList<>());
			 final RocksDB rocksDB2 = RocksDB.open(tempFolder.newFolder().getAbsolutePath());
			 final RocksDB rocksDB3 = RocksDB.open(tempFolder.newFolder().getAbsolutePath());
			 final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
			 final ColumnFamilyHandle testHandleForRocksDB1 = rocksDB1.createColumnFamily(
				 new ColumnFamilyDescriptor("test".getBytes()));
			 final ColumnFamilyHandle testHandleForRocksDB2 = rocksDB2.createColumnFamily(
				 new ColumnFamilyDescriptor("test".getBytes()));
			 final ColumnFamilyHandle testHandleForRocksDB3 = rocksDB3.createColumnFamily(
				 new ColumnFamilyDescriptor("test".getBytes()))) {

			ColumnFamilyHandle[] handlesForRocksDB1 = new ColumnFamilyHandle[1];
			handlesForRocksDB1[0] = testHandleForRocksDB1;

			ColumnFamilyHandle[] handlesForRocksDB2 = new ColumnFamilyHandle[1];
			handlesForRocksDB2[0] = testHandleForRocksDB2;

			ColumnFamilyHandle[] handlesForRocksDB3 = new ColumnFamilyHandle[1];
			handlesForRocksDB3[0] = testHandleForRocksDB3;

			SimpleKeyGroupIterable keyGroupIterable1 = new SimpleKeyGroupIterable(handlesForRocksDB2, 0, 10_000_000);

			long t1 = System.currentTimeMillis();
			Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterator = keyGroupIterable1.iterator();
			while (iterator.hasNext()) {
				Tuple3<ColumnFamilyHandle, byte[], byte[]> item = iterator.next();
				rocksDB1.put(testHandleForRocksDB1, writeOptions, item.f1, item.f2);
			}
			long t2 = System.currentTimeMillis();
			System.out.println("cost with RocksDB.put():" + (t2 - t1));
			testHandleForRocksDB1.close();
			rocksDB1.close();

			// with parallelism 1
			SstFileBuilder sstFileBuilder = new SstFileBuilder(
				tempFolder.newFolder().getAbsolutePath(),
				32 * 1024 * 1024,
				executorService,
				new SimpleSstHandle(rocksDB2));

			long t3 = System.currentTimeMillis();
			sstFileBuilder.building(keyGroupIterable1).get();
			long t4 = System.currentTimeMillis();
			System.out.println("cost with parallelism 1:" + (t4 - t3));
			testHandleForRocksDB2.close();
			rocksDB2.close();

			keyGroupIterable1.getRecords().clear();

			SimpleKeyGroupIterable keyGroupIterable2 = new SimpleKeyGroupIterable(handlesForRocksDB3, 0, 2_500_000);
			SimpleKeyGroupIterable keyGroupIterable3 = new SimpleKeyGroupIterable(handlesForRocksDB3, 2_500_000, 5_000_000);
			SimpleKeyGroupIterable keyGroupIterable4 = new SimpleKeyGroupIterable(handlesForRocksDB3, 5_000_000, 7_500_000);
			SimpleKeyGroupIterable keyGroupIterable5 = new SimpleKeyGroupIterable(handlesForRocksDB3, 7_500_000, 10_000_000);

			// with parallelism 2
			SstFileBuilder sstFileBuilder2 = new SstFileBuilder(
				tempFolder.newFolder().getAbsolutePath(),
				64 * 1024 * 1024,
				executorService,
				new SimpleSstHandle(rocksDB3));

			SstFileBuilder sstFileBuilder3 = new SstFileBuilder(
				tempFolder.newFolder().getAbsolutePath(),
				64 * 1024 * 1024,
				executorService,
				new SimpleSstHandle(rocksDB3));

			SstFileBuilder sstFileBuilder4 = new SstFileBuilder(
				tempFolder.newFolder().getAbsolutePath(),
				64 * 1024 * 1024,
				executorService,
				new SimpleSstHandle(rocksDB3));

			SstFileBuilder sstFileBuilder5 = new SstFileBuilder(
				tempFolder.newFolder().getAbsolutePath(),
				64 * 1024 * 1024,
				executorService,
				new SimpleSstHandle(rocksDB3));

			long t5 = System.currentTimeMillis();

			CompletableFuture completableFuture2 = sstFileBuilder2.building(keyGroupIterable2);
			CompletableFuture completableFuture3 = sstFileBuilder3.building(keyGroupIterable3);
			CompletableFuture completableFuture4 = sstFileBuilder4.building(keyGroupIterable4);
			CompletableFuture completableFuture5 = sstFileBuilder5.building(keyGroupIterable5);

			completableFuture2.get();
			completableFuture3.get();
			completableFuture4.get();
			completableFuture5.get();

			long t6 = System.currentTimeMillis();

			System.out.println("cost with parallelism 4:" + (t6 - t5));
			testHandleForRocksDB3.close();
			rocksDB3.close();
		}
	}

	class SimpleSstHandle implements BiFunction<ColumnFamilyHandle, String, Boolean> {

		private final RocksDB rocksDB;

		public SimpleSstHandle(RocksDB rocksDB) {
			this.rocksDB = rocksDB;
		}

		@Override
		public Boolean apply(ColumnFamilyHandle handle, String path) {
			try {
				synchronized (rocksDB) {
					rocksDB.ingestExternalFile(
						handle,
						Collections.singletonList(path),
						new IngestExternalFileOptions(true, true, true, true));
				}
				return true;
			} catch (Exception ex) {
				return false;
			}
		}
	}

	class SimpleKeyGroupIterable implements CloseableIterable<Tuple3<ColumnFamilyHandle, byte[], byte[]>> {

		private final List<Tuple3<ColumnFamilyHandle, byte[], byte[]>> records = new ArrayList<>(1024);

		public SimpleKeyGroupIterable(ColumnFamilyHandle[] handles, int begin, int end) {
			for (int i = begin; i < end; ++i) {
				records.add(new Tuple3<>(
					handles[new Random().nextInt(handles.length)],
					intToBytes(i),
					String.format("the value for %d-th key", i).getBytes()));
			}
		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterator() {
			return new Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>>() {

				private int i = 0;

				@Override
				public boolean hasNext() {
					return i < records.size();
				}

				@Override
				public Tuple3<ColumnFamilyHandle, byte[], byte[]> next() {
					assert hasNext();
					return records.get(i++);
				}
			};
		}

		public List<Tuple3<ColumnFamilyHandle, byte[], byte[]>> getRecords() {
			return records;
		}
	}

	static byte[] intToBytes(int x) {
		byte[] bytes = new byte[4];
		for (int i = 4; --i > 0; x >>>= 8)
			bytes[i] = (byte) (x & 0xFF);
		return bytes;
	}
}
