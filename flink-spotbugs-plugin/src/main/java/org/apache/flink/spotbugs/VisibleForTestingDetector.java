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


import org.apache.flink.annotation.VisibleForTesting;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.BytecodeScanningDetector;
import edu.umd.cs.findbugs.ba.XClass;
import edu.umd.cs.findbugs.ba.XMethod;
import edu.umd.cs.findbugs.classfile.CheckedAnalysisException;
import edu.umd.cs.findbugs.classfile.ClassDescriptor;
import edu.umd.cs.findbugs.classfile.DescriptorFactory;

/**
 * A detector to ensure that implementation (class in src/main/java) doesn't call
 * package-private method in other class which is annotated by {@See @VisibleForTesting}.
 *
 * @see org.apache.flink.annotation.VisibleForTesting
 */
public class VisibleForTestingDetector extends BytecodeScanningDetector {
	private static final ClassDescriptor VISIBLE_FOR_TESTING_ANNOTATION =
		DescriptorFactory.createClassDescriptor(VisibleForTesting.class);

	private final BugReporter bugReporter;

	public VisibleForTestingDetector(BugReporter bugReporter) {
		this.bugReporter = bugReporter;
	}

	@Override
	public void sawMethod() {
		checkMethod();
	}

	@Override
	public void sawIMethod() {
		checkMethod();
	}

	private void checkMethod() {

		XClass currentClass = getXClass();
		XMethod currentMethod = getXMethod();

		XClass calledClass = getXClassOperand();
		XMethod calledMethod = getXMethodOperand();
		if ((calledMethod != null && calledMethod.getAnnotation(VISIBLE_FOR_TESTING_ANNOTATION) != null) ||
			(calledClass != null && calledClass.getAnnotation(VISIBLE_FOR_TESTING_ANNOTATION) != null)) {

			if (currentClass.equals(calledClass)) {
				return;
			}

			if (!isTestClass(currentClass)) {
				BugInstance bug = new BugInstance(this, "VISIBLE_FOR_TESTING", HIGH_PRIORITY)
					.addCalledMethod(this).addClassAndMethod(this).addSourceLine(this);
				bugReporter.reportBug(bug);

			}
		}
	}

	private boolean isTestClass(XClass xClass) {
		String className = xClass.getClassDescriptor().getSimpleName();
		if (className.endsWith("Test") || className.endsWith("ITCase")) {
			return true;
		}

		ClassDescriptor superClass = xClass.getSuperclassDescriptor();
		if (superClass != null && !superClass.getSimpleName().equals(Object.class.getSimpleName())) {
			try {
				return isTestClass(superClass.getXClass());
			} catch (CheckedAnalysisException e) {
				bugReporter.logError("Error looking up " + className, e);
			}
		}
		return false;
	}
}
