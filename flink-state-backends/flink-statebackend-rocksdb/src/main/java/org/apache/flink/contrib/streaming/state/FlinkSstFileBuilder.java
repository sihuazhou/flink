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
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileWriter;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

/**
 * Class to build sst files from the iterator.
 */
public class FlinkSstFileBuilder {

	private Map<ColumnFamilyHandle, FlinkSstFileWriter> writers = new HashMap<>(4);

	private final ExecutorService executorService;
	private CompletableFuture completableFuture;
	private final int sstFileSize;
	private final String[] basePaths;

	private final BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler;

	public FlinkSstFileBuilder(
		String[] basePaths,
		int sstFileSize,
		ExecutorService executorService,
		BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler) {
		this.sstFileSize = sstFileSize;
		this.executorService = executorService;
		this.basePaths = basePaths;
		this.onSstGeneratedHandler = onSstGeneratedHandler;
	}

	public CompletableFuture building(Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterator) {

		assert completableFuture == null;

		completableFuture = new CompletableFuture();

		executorService.submit(() -> {
				try {
					while (iterator.hasNext()) {
						Tuple3<ColumnFamilyHandle, byte[], byte[]> item = iterator.next();
						ColumnFamilyHandle handle = item.f0;
						byte[] key = item.f1;
						byte[] value = item.f2;

						addRecord(handle, key, value);

					}
				} catch (Exception ex) {
					completableFuture.completeExceptionally(ex);
				}
		});

		return completableFuture;
	}

	void addRecord(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {

		FlinkSstFileWriter writer = writers.get(columnFamilyHandle);
		if (writer == null) {
			writer = new FlinkSstFileWriter(
				"",
				sstFileSize,
				columnFamilyHandle,
				onSstGeneratedHandler);
			writers.put(columnFamilyHandle, writer);
		}

		writer.addRecord(key, value);
	}

	/**
	 * Helper class to generate sst file base on the {@link #sstFileSize},
	 * once a sst file generated it invokes the {@link #onSstGeneratedHandler} to handle it.
	 */
	public static class FlinkSstFileWriter {

		private final BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler;
		private final long sstFileSize;
		private SstFileWriter writer;
		private final ColumnFamilyHandle columnFamilyHandle;
		private final String basePath;
		private String currentFilePath;

		// we perform a size check for every 10 records.
		private static int SIZE_CHECK_INTERVAL = 100;
		private int currentRecords;

		public FlinkSstFileWriter(
			String basePath,
			long sstFileSize,
			ColumnFamilyHandle columnFamilyHandle,
			BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler) throws RocksDBException {
			this.columnFamilyHandle = columnFamilyHandle;
			this.sstFileSize = sstFileSize;
			this.basePath = basePath + "/sstFileWriter/" + UUID.randomUUID();
			this.onSstGeneratedHandler = onSstGeneratedHandler;
			initWriter();
		}

		private String generateNewFile() {
			int trys = 10;
			while(trys > 0) {
				String tmpFilePath = basePath + "/" + UUID.randomUUID().toString() + ".sst";
				if (!new File(tmpFilePath).exists()) {
					return tmpFilePath;
				}
			}
			throw new RuntimeException("Failed to generate new sst file.");
		}

		private void initWriter() throws RocksDBException {
			this.writer = new SstFileWriter(new EnvOptions(), new Options());
			this.currentFilePath = generateNewFile();
			this.writer.open(this.currentFilePath);
		}

		private boolean needToFinish() {
			File file = new File(this.currentFilePath);
			long length = file.length();
			if (length >= sstFileSize) {
				return true;
			}
			return false;
		}

		public void addRecord(byte[] key, byte[] value) throws RocksDBException {

			writer.put(new Slice(key), new Slice(value));

			if ((++currentRecords % SIZE_CHECK_INTERVAL) == 0) {
				if (needToFinish()) {
					writer.finish();
					writer.close();

					// handle the generated sst file
					if (!onSstGeneratedHandler.apply(this.columnFamilyHandle, this.currentFilePath)) {
						throw new RuntimeException("");
					}

					this.currentRecords = 0;
					initWriter();
				}
			}
		}

		public void dispose() {
			//clean up
		}
	}
}
