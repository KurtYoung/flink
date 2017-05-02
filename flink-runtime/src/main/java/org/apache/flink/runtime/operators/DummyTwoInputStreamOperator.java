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

import org.apache.flink.util.Collector;

public class DummyTwoInputStreamOperator<T> implements FakeTwoInputStreamOperator<T, T, T> {

	private Collector<T> output;

	@Override
	public void setup(Collector<T> output) {
		this.output = output;
	}

	@Override
	public void open() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void dispose() throws Exception {

	}

	@Override
	public InputSelection firstInputSelection() {
		return InputSelection.FIRST;
	}

	@Override
	public InputSelection processElement1(T element) throws Exception {
		output.collect(element);
		return InputSelection.FIRST;
	}

	@Override
	public InputSelection processElement2(T element) throws Exception {
		output.collect(element);
		return InputSelection.SECOND;
	}

	@Override
	public InputSelection endInput1() throws Exception {
		return InputSelection.SECOND;
	}

	@Override
	public InputSelection endInput2() throws Exception {
		return InputSelection.NONE;
	}
}
