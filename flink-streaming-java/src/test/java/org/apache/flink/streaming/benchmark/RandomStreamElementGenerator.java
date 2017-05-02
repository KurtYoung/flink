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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.util.MutableObjectIterator;

public class RandomStreamElementGenerator<T> implements MutableObjectIterator<StreamElement> {

	private final MutableObjectIterator<T> iterator;

	private final StreamElementSerializer<T> serializer;

	private final T reuseRecord;

	public RandomStreamElementGenerator(
			MutableObjectIterator<T> iterator,
			TypeSerializer<T> typeSerializer) {
		this.iterator = iterator;
		this.serializer = new StreamElementSerializer<>(typeSerializer);
		this.reuseRecord = typeSerializer.createInstance();
	}

	@Override
	public StreamElement next(StreamElement reuse) throws IOException {
		T record = iterator.next(reuseRecord);
		if (record != null) {
			reuse.asRecord().replace(record);
			return reuse;
		} else {
			return null;
		}
	}

	@Override
	public StreamElement next() throws IOException {
		StreamElement record = serializer.createInstance();
		return next(record);
	}
}
