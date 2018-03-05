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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DirectSlice;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileWriter;
import scala.Array;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
public class SstFileBuilder {

	private Map<ColumnFamilyHandle, SstFileWriterWrapper> writers = new HashMap<>(4);

	private final ExecutorService executorService;
	private CompletableFuture completableFuture;
	private final long sstFileSize;
	private final String basePath;

	private final BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler;

	public SstFileBuilder(
		String basePath,
		long sstFileSize,
		ExecutorService executorService,
		BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler) {
		this.sstFileSize = sstFileSize;
		this.executorService = executorService;
		this.basePath = basePath;
		this.onSstGeneratedHandler = onSstGeneratedHandler;
	}

	public CompletableFuture building(CloseableIterable<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterable) {

		Preconditions.checkArgument(completableFuture == null, "CompletableFuture is not null.");

		completableFuture = CompletableFuture.runAsync(() -> {
			try {
				try {
					Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterator = iterable.iterator();
					while (iterator.hasNext()) {
						Tuple3<ColumnFamilyHandle, byte[], byte[]> item = iterator.next();
						ColumnFamilyHandle handle = item.f0;
						byte[] key = item.f1;
						byte[] value = item.f2;

						addRecord(handle, key, value);
					}

					for (SstFileWriterWrapper writerWrapper : writers.values()) {
						writerWrapper.finish();
					}
				} finally {
					iterable.close();
				}
			} catch (Exception ex) {
				completableFuture.completeExceptionally(ex);
			}
		}, executorService);

		return completableFuture;
	}

	private void addRecord(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
		throws RocksDBException, IOException {

		SstFileWriterWrapper writer = writers.get(columnFamilyHandle);
		if (writer == null) {
			writer = new SstFileWriterWrapper(
				basePath,
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
	public static class SstFileWriterWrapper {

		private final BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler;
		private final long sstFileSize;
		private SstFileWriter writer;
		private final ColumnFamilyHandle columnFamilyHandle;
		private final String basePath;
		private String currentFilePath;

		private long currentSize = 0;

		private ByteBuffer keyByteBuffer;
		private ByteBuffer valueByteBuffer;

		public SstFileWriterWrapper(
			String basePath,
			long sstFileSize,
			ColumnFamilyHandle columnFamilyHandle,
			BiFunction<ColumnFamilyHandle, String, Boolean> onSstGeneratedHandler)
			throws RocksDBException, IOException {
			this.columnFamilyHandle = columnFamilyHandle;
			this.sstFileSize = sstFileSize;

			// init the base directory
			this.basePath = basePath + "/" + UUID.randomUUID();
			File baseDir = new File(this.basePath);
			if (!baseDir.exists()) {
				if (baseDir.mkdir()) {
					//
				}
			}

			this.onSstGeneratedHandler = onSstGeneratedHandler;
			initWriter();

			keyByteBuffer = ByteBuffer.allocateDirect(4096);
			valueByteBuffer = ByteBuffer.allocateDirect(4096);
		}

		private String generateNewFile() throws IOException {
			int tries = 10;
			while(tries > 0) {
				String tmpFilePath = basePath + "/" + UUID.randomUUID().toString() + ".sst";
				File tmpFile = new File(tmpFilePath);
				if (!tmpFile.exists()) {
					tmpFile.createNewFile();
					return tmpFilePath;
				}
			}
			throw new RuntimeException("Failed to generate new sst file.");
		}

		private void initWriter() throws RocksDBException, IOException {

			this.writer = new SstFileWriter(new EnvOptions(), new Options());
			this.currentFilePath = generateNewFile();
			this.writer.open(this.currentFilePath);
		}

		public void addRecord(byte[] key, byte[] value) throws RocksDBException, IOException {

			if (keyByteBuffer.capacity() >= key.length) {
				keyByteBuffer.clear();
			} else {
				keyByteBuffer = ByteBuffer.allocate(key.length);
				keyByteBuffer.flip();
			}
			keyByteBuffer.put(key, 0, key.length);

			if (valueByteBuffer.capacity() >= value.length) {
				valueByteBuffer.clear();
			} else {
				valueByteBuffer = ByteBuffer.allocate(value.length);
				valueByteBuffer.flip();
			}
			valueByteBuffer.put(value, 0, value.length);

			writer.put(new DirectSlice(keyByteBuffer, key.length), new DirectSlice(valueByteBuffer, value.length));

			currentSize += key.length + value.length;

			if (currentSize >= sstFileSize) {
				writer.finish();
				writer.close();

				// handle the generated sst file
				if (!onSstGeneratedHandler.apply(this.columnFamilyHandle, this.currentFilePath)) {
					throw new FlinkRuntimeException("Failed to handle sst file.");
				}

				this.currentSize = 0;
				initWriter();
			}
		}

		public void finish() throws RocksDBException {
			if (currentSize > 0) {
				writer.finish();
				writer.close();

				if (!onSstGeneratedHandler.apply(this.columnFamilyHandle, this.currentFilePath)) {
					throw new FlinkRuntimeException("Failed to handle sst file.");
				}
			}
		}
	}
}
