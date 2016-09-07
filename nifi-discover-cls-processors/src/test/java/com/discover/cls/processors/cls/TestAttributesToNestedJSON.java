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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class TestAttributesToNestedJSON {
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

    private static final Map<String, String> ATTRIBUTES = new HashMap<String, String>() {{
        put("_id", "57cf1fcc583d8f04fdc04902");
        put("index", "0");
        put("name", "{ \"first\": \"Donna\", \"last\": \"Hardin\" }");
        put("tags", "[ \"culpa\", \"sunt\", \"reprehenderit\", \"fugiat\", \"velit\" ]");
        put("greeting", "\"Hello, Donna! You have 9 unread messages.\"");
        put("favoriteFruit", "\"strawberry\"");
    }};

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(AttributesToNestedJSON.class);
    }

    @Ignore
    @Test
    public void testProcessor() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        testRunner.setProperty(AttributesToNestedJSON.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, "flowfile-content");

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, ATTRIBUTES);

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JsonToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JsonToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            file.assertContentEquals(TEST_INPUT);
        }
    }

}
