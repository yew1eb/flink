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

package org.apache.flink.spotbugs;

import edu.umd.cs.findbugs.BugCollection;
import edu.umd.cs.findbugs.test.SpotBugsRule;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcher;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcherBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static edu.umd.cs.findbugs.test.SpotBugsRule.containsExactly;
import static org.junit.Assert.assertThat;

/**
 * VisibleForTestingDetector Tester.
 */
public class VisibleForTestingDetectorTest {
	@Rule
	public SpotBugsRule spotbugs = new SpotBugsRule();

	@Test
	public void testCallingVisibleClassForTesting() {
		Path path1 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "ClassWhichCallsVisibleClassForTesting.class");
		Path path2 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "ClassWithVFT.class");

		BugCollection bugCollection = spotbugs.performAnalysis(path1, path2);

		BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
			.bugType("VISIBLE_FOR_TESTING").build();
		assertThat(bugCollection, containsExactly(bugTypeMatcher, 2));
	}


	@Test
	public void testCallingVisibleMethodForTesting() {
		Path path1 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "ClassWhichCallsVisibleMethodForTesting.class");
		Path path2 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "MethodWithVFT.class");

		BugCollection bugCollection = spotbugs.performAnalysis(path1, path2);

		BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
			.bugType("VISIBLE_FOR_TESTING").build();

		assertThat(bugCollection, containsExactly(bugTypeMatcher, 1));
	}


	@Test
	public void testCallingNormalMethod() {
		Path path1 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "ClassWhichCallsNormalMethod.class");
		Path path2 = Paths.get("target/test-classes", "org.apache.flink.spotbugs".replace('.', '/'), "MethodWithoutVFT.class");

		BugCollection bugCollection = spotbugs.performAnalysis(path1, path2);

		BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
			.bugType("VISIBLE_FOR_TESTING").build();

		assertThat(bugCollection, containsExactly(bugTypeMatcher, 0));
	}
}
