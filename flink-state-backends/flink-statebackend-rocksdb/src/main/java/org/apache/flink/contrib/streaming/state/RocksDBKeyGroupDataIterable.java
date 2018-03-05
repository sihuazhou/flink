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

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * This class is not thread safe.
 */
public class RocksDBKeyGroupDataIterable implements CloseableIterable<Tuple3<ColumnFamilyHandle, byte[], byte[]>> {
	private final FSDataInputStream stateHandleInStream;
	private final KeyGroupRange targetKeyGroupRange;
	private final Iterator<Tuple2<Integer, Long>> keyGroupOffsetIterator;
	private final List<ColumnFamilyHandle> columnFamilies;
	private final StreamCompressionDecorator keygroupStreamCompressionDecorator;

	private Tuple2<Integer, Long> currentKeyGroupOffset = null;
	private boolean currentKeyGroupHasMoreKeys = true;
	private ColumnFamilyHandle currentColumnFamilyHandle = null;
	private boolean isNewKeyGroup = true;
	private Tuple3<ColumnFamilyHandle, byte[], byte[]> nextItem = null;

	public RocksDBKeyGroupDataIterable(FSDataInputStream stateHandleInStream,
									   KeyGroupRange targetKeyGroupRange,
									   Iterator<Tuple2<Integer, Long>> keyGroupOffsetIterator,
									   List<ColumnFamilyHandle> columnFamilies,
									   StreamCompressionDecorator keygroupStreamCompressionDecorator) {

		this.stateHandleInStream = stateHandleInStream;
		this.targetKeyGroupRange = targetKeyGroupRange;
		this.keyGroupOffsetIterator = keyGroupOffsetIterator;
		this.columnFamilies = columnFamilies;
		this.keygroupStreamCompressionDecorator = keygroupStreamCompressionDecorator;
	}

	@Override
	public Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>> iterator() {

		return new Iterator<Tuple3<ColumnFamilyHandle, byte[], byte[]>>() {

			@Override
			public boolean hasNext() {

				if (nextItem != null) {
					try {
						startNewKeyGroup();

						try (InputStream compressedKgIn = keygroupStreamCompressionDecorator.decorateWithCompression(stateHandleInStream)) {
							DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);

							byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
							byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);

							if (isNewKeyGroup) {
								int kvStateId = RocksDBKeyedStateBackend.RocksDBFullSnapshotOperation.END_OF_KEY_GROUP_MARK & compressedKgInputView.readShort();
								currentColumnFamilyHandle = columnFamilies.get(kvStateId);
								isNewKeyGroup = false;
							} else if (RocksDBKeyedStateBackend.RocksDBFullSnapshotOperation.hasMetaDataFollowsFlag(key)) {
								//clear the signal bit in the key to make it ready for insertion again
								RocksDBKeyedStateBackend.RocksDBFullSnapshotOperation.clearMetaDataFollowsFlag(key);
								int kvStateId = RocksDBKeyedStateBackend.RocksDBFullSnapshotOperation.END_OF_KEY_GROUP_MARK & compressedKgInputView.readShort();
								if (RocksDBKeyedStateBackend.RocksDBFullSnapshotOperation.END_OF_KEY_GROUP_MARK == kvStateId) {
									currentKeyGroupHasMoreKeys = false;
								} else {
									currentColumnFamilyHandle = columnFamilies.get(kvStateId);
								}
							}
							nextItem = new Tuple3<>(currentColumnFamilyHandle, key, value);
						} catch (Exception e) {

						}
					} catch (Exception e) {

					}
				}

				return true;
			}

			@Override
			public Tuple3<ColumnFamilyHandle, byte[], byte[]> next() {
				if (!hasNext()) {
					throw new RuntimeException("");
				}

				Tuple3<ColumnFamilyHandle, byte[], byte[]> tmpNextItem = nextItem;
				nextItem = null;

				return tmpNextItem;
			}

			private boolean startNewKeyGroup() throws IOException {

				while (!currentKeyGroupHasMoreKeys && keyGroupOffsetIterator.hasNext()) {

					currentKeyGroupOffset = keyGroupOffsetIterator.next();
					int keyGroup = currentKeyGroupOffset.f0;

					// Check that restored key groups all belong to the backend
					Preconditions.checkState(targetKeyGroupRange.contains(keyGroup), "The key group must belong to the backend");

					long offset = currentKeyGroupOffset.f1;

					//not empty key-group?
					if (0L != offset) {
						stateHandleInStream.seek(offset);
						isNewKeyGroup = true;
						currentKeyGroupHasMoreKeys = true;
						return true;
					}
				}
				return false;
			}
		};
	}

	@Override
	public void close() throws IOException {
	}
}
