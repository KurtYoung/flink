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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class DummyTwoInputDriver<IN> implements Driver<MapFunction<IN, IN>, IN> {

	private TaskContext<MapFunction<IN, IN>, IN> context;

	@Override
	public void setup(TaskContext<MapFunction<IN, IN>, IN> context) {
		this.context = context;
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public Class<MapFunction<IN, IN>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<MapFunction<IN, IN>> clazz = (Class<MapFunction<IN, IN>>) (Class<?>) MapFunction.class;
		return clazz;
	}

	@Override
	public void prepare() throws Exception {

	}

	@Override
	public void run() throws Exception {
		final MutableObjectIterator<IN> in1 = this.context.getInput(0);
		final MutableObjectIterator<IN> in2 = this.context.getInput(1);
		final MapFunction<IN, IN> function = this.context.getStub();
		final Collector<IN> output = this.context.getOutputCollector();

		IN record = this.context.<IN>getInputSerializer(0).getSerializer().createInstance();
		while ((record = in1.next(record)) != null) {
			output.collect(function.map(record));
		}
		record = this.context.<IN>getInputSerializer(0).getSerializer().createInstance();
		while ((record = in2.next(record)) != null) {
			output.collect(function.map(record));
		}
	}

	@Override
	public void cleanup() throws Exception {

	}

	@Override
	public void cancel() throws Exception {

	}
}
