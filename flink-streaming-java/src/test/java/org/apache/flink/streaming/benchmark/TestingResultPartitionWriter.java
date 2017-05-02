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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.util.event.EventListener;

public class TestingResultPartitionWriter
		extends ResultPartitionWriter
		implements BufferProvider, BufferRecycler {

	private final AtomicLong count;

	private volatile MemorySegment memorySegment;

	public TestingResultPartitionWriter() {
		super(null);
		this.count = new AtomicLong();
		this.memorySegment = MemorySegmentFactory.wrap(new byte[32 * 1024]);
	}

	@Override
	public BufferProvider getBufferProvider() {
		return this;
	}

	@Override
	public int getNumberOfOutputChannels() {
		return 1;
	}

	@Override
	public void writeBuffer(Buffer buffer, int targetChannel) throws IOException {
		count.addAndGet(1);
	}

	@Override
	public void writeBufferToAllChannels(Buffer eventBuffer) throws IOException {
		count.addAndGet(1);
	}

	public AtomicLong getCount() {
		return count;
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		return new Buffer(memorySegment, this);
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		return new Buffer(memorySegment, this);
	}

	@Override
	public boolean addListener(EventListener<Buffer> listener) {
		return false;
	}

	@Override
	public boolean isDestroyed() {
		return false;
	}

	@Override
	public int getMemorySegmentSize() {
		return 32 * 1024;
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		this.memorySegment = memorySegment;
	}
}
