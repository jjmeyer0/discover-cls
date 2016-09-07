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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;


public class JsonToAttributesTest {
    private static final String TEST_INPUT = "{\n" +
            "  \"_id\": \"57cf1fcc583d8f04fdc04902\",\n" +
            "  \"index\": 0,\n" +
            "  \"name\": {\n" +
            "    \"first\": \"Donna\",\n" +
            "    \"last\": \"Hardin\"\n" +
            "  },\n" +
            "  \"tags\": [\n" +
            "    \"culpa\",\n" +
            "    \"sunt\",\n" +
            "    \"reprehenderit\",\n" +
            "    \"fugiat\",\n" +
            "    \"velit\"\n" +
            "  ],\n" +
            "  \"greeting\": \"Hello, Donna! You have 9 unread messages.\",\n" +
            "  \"favoriteFruit\": \"strawberry\"\n" +
            "}";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JsonToAttributes.class);
    }

    @Test
    public void testProcessor() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(TEST_INPUT.getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JsonToAttributeList.REL_SUCCESS);
    }

}