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

package org.apache.flink.streaming.api.operators;

/**
 *
 */
public class InputSelection {

	public static final InputSelection NONE = new InputSelection(-2);

	public static final InputSelection RANDOM = new InputSelection(-1);

	public static final InputSelection FIRST = new InputSelection(0);

	public static final InputSelection SECOND = new InputSelection(1);

	private final int selection;

	public InputSelection(int selection) {
		this.selection = selection;
	}

	public int getSelection() {
		return selection;
	}
}
