/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.discover.cls.processors.cls;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.junit.Assert.*;


public class JsonToAttributeListTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JsonToAttributeList.class);
    }

    @Test(expected = AssertionError.class)
    public void attributeListMustBeDefined() {
        testRunner.setProperty(JsonToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        testRunner.enqueue(flowFile);
        testRunner.run();
    }

    @Test
    public void processorShouldProperlyConvertJsonToAttributes() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":\"val1\",\"key2\":\"val2\"}".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JsonToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JsonToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("key1,key2", file.getAttribute(JsonToAttributeList.ATTRIBUTE_LIST));
        }
    }
}
