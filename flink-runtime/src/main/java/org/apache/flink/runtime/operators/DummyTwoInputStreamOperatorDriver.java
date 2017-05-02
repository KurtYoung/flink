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
import org.apache.flink.util.MutableObjectIterator;

public class DummyTwoInputStreamOperatorDriver<IN> implements Driver<MapFunction<IN, IN>, IN> {

	private TaskContext<MapFunction<IN, IN>, IN> context;

	private DummyTwoInputStreamOperator<IN> fakeOp;

	private boolean input1Finished;

	private boolean input2Finished;

	public DummyTwoInputStreamOperatorDriver() {
		this.fakeOp = new DummyTwoInputStreamOperator<>();
		this.input1Finished = false;
		this.input2Finished = false;
	}

	@Override
	public void setup(TaskContext<MapFunction<IN, IN>, IN> context) {
		this.context = context;
		this.fakeOp.setup(context.getOutputCollector());
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

		IN record = this.context.<IN>getInputSerializer(0).getSerializer().createInstance();
		final MapFunction<IN, IN> function = this.context.getStub();

		InputSelection selection = fakeOp.firstInputSelection();
		do {
			if (selection.equals(InputSelection.NONE)) {
				break;
			}
			if (selection.equals(InputSelection.FIRST)) {
				record = in1.next(record);
				if (record == null) {
					input1Finished = true;
					selection = fakeOp.endInput1();
					record = this.context.<IN>getInputSerializer(0).getSerializer().createInstance();
				} else {
					selection = fakeOp.processElement1(function.map(record));
				}
			} else if (selection.equals(InputSelection.SECOND)) {
				record = in2.next(record);
				if (record == null) {
					input2Finished = true;
					selection = fakeOp.endInput2();
					record = this.context.<IN>getInputSerializer(0).getSerializer().createInstance();
				} else {
					selection = fakeOp.processElement2(function.map(record));
				}
			} else {
				throw new IllegalStateException("");
			}
		} while (!input1Finished || !input2Finished);
	}

	@Override
	public void cleanup() throws Exception {

	}

	@Override
	public void cancel() throws Exception {

	}
}
