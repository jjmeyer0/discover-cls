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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
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
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestAttributesToNestedJSON {
    private static final String TEST_ATTRIBUTE_KEY = "TestAttribute";
    private static final String TEST_ATTRIBUTE_VALUE = "TestValue";

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
        put("_id", "\"57cf1fcc583d8f04fdc04902\"");
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

    @Test
    public void processorShouldProperlyConvertAttributesToJsonAndIncludeCoreAttributes() throws Exception {
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
            Map<String, Object> expected = new ObjectMapper().readValue(TEST_INPUT, HashMap.class);
            Map<String, Object> result = new ObjectMapper().readValue(new String(file.toByteArray()), HashMap.class);

            for (String key : expected.keySet()) {
                assertEquals(expected.get(key), result.get(key));
            }

            for (CoreAttributes coreAttribute : CoreAttributes.values()) {
                if (coreAttribute == CoreAttributes.MIME_TYPE) {
                    // only verify that the flow file has the mime type. this is added after content is written.
                    assertEquals("application/json", file.getAttribute(coreAttribute.key()));
                } else {
                    assertEquals(result.get(coreAttribute.key()), file.getAttributes().get(coreAttribute.key()));
                }
            }
        }
    }

    ///
    /// The tests below are taken from org.apache.nifi.processors.standard.TestAttributesToJSON
    ///

    @Test(expected = AssertionError.class)
    public void testInvalidUserSuppliedAttributeList() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());

        //Attribute list CANNOT be empty
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, "");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIncludeCoreAttributesProperty() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, "val1,val2");
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(AttributesToNestedJSON.INCLUDE_CORE_ATTRIBUTES, "maybe");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();
    }

    @Test
    public void testNullValueForEmptyAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToNestedJSON.NULL_VALUE_FOR_EMPTY_STRING, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non existing attribute
        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        assertNull(val.get(NON_PRESENT_ATTRIBUTE_KEY));
    }

    @Test
    public void testEmptyStringValueForEmptyAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToNestedJSON.NULL_VALUE_FOR_EMPTY_STRING, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non existing attribute
        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        assertEquals(val.get(NON_PRESENT_ATTRIBUTE_KEY), "");
    }

    @Test
    public void testInvalidJSONValueInAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        //Create attribute that contains an invalid JSON Character
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, "'badjson'");

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);
    }

    @Test
    public void testAttributes_emptyListUserSpecifiedAttributes() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
    }

    @Test
    public void testContent_emptyListUserSpecifiedAttributes() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_CONTENT);
        testRunner.setProperty(AttributesToNestedJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeNotExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);
        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0).assertContentEquals("{}");
    }

    @Test
    public void testAttribute_singleUserDefinedAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, TEST_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
        assertTrue(val.size() == 1);
    }

    @Test
    public void testAttribute_singleUserDefinedAttributeWithWhiteSpace() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, " " + TEST_ATTRIBUTE_KEY + " ");
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
        assertTrue(val.size() == 1);
    }

    @Test
    public void testAttribute_singleNonExistingUserDefinedAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToNestedJSON());
        testRunner.setProperty(AttributesToNestedJSON.ATTRIBUTES_LIST, "NonExistingAttribute");
        testRunner.setProperty(AttributesToNestedJSON.DESTINATION, AttributesToNestedJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToNestedJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToNestedJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToNestedJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        //If a Attribute is requested but does not exist then it is placed in the JSON with an empty string
        assertTrue(val.get("NonExistingAttribute").equals(""));
        assertTrue(val.size() == 1);
    }
}
