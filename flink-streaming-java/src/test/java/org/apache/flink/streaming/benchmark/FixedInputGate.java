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

package org.apache.flink.streaming.benchmark;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class FixedInputGate<T> implements InputGate, BufferRecycler {

	private int count;

	private Buffer buffer;

	private BufferOrEvent bufferOrEvent;

	private int generatedCount = 0;

	public FixedInputGate(int count, T record, TypeSerializer<T> inputSerializer) throws Exception {
		this.count = count;

		SerializationDelegate<T> delegate = new SerializationDelegate<>(inputSerializer);
		delegate.setInstance(record);

		MemorySegment memorySegment = MemorySegmentFactory.wrap(new byte[32 * 1024]);
		this.buffer = new Buffer(memorySegment, this);

		RecordSerializer<SerializationDelegate<T>> serializer = new SpanningRecordSerializer<>();
		serializer.setNextBuffer(buffer);
		serializer.addRecord(delegate);
		this.bufferOrEvent = new BufferOrEvent(serializer.getCurrentBuffer(), 0);
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
	}

	@Override
	public int getNumberOfInputChannels() {
		return 1;
	}

	@Override
	public boolean isFinished() {
		return generatedCount >= count;
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
	}

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
		if (generatedCount < count) {
			generatedCount++;
			buffer.retain();
			return bufferOrEvent;
		} else {
			return null;
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {

	}

	@Override
	public void registerListener(InputGateListener listener) {

	}

	@Override
	public int getPageSize() {
		return 32 * 1024;
	}

	public int getProcessedCount() {
		return generatedCount;
	}
}
