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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestJSONToAttributes {
    private static final String TEST_INPUT = "{\n" +
            "  \"_id\": \"57cf1fcc583d8f04fdc04902\",\n" +
            "  \"index\": 0,\n" +
            "  \"has_index\": true,\n" +
            "  \"test_null\": null,\n" +
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

    static final String JSON_TEST = "{\n" +
            "   \"Port\":\n" +
            "   {\n" +
            "       \"@alias\": \"defaultHttp\",\n" +
            "       \"Enabled\": \"true\",\n" +
            "       \"Number\": \"10092\",\n" +
            "       \"Protocol\": \"http\",\n" +
            "       \"KeepAliveTimeout\": \"20000\",\n" +
            "       \"ThreadPool\":\n" +
            "       {\n" +
            "           \"@enabled\": \"false\",\n" +
            "           \"Max\": \"150\",\n" +
            "           \"ThreadPriority\": \"5\"\n" +
            "       },\n" +
            "       \"ExtendedProperties\":\n" +
            "       {\n" +
            "           \"Property\":\n" +
            "           [                         \n" +
            "               {\n" +
            "                   \"@name\": \"connectionTimeout\",\n" +
            "                   \"$\": \"20000\"\n" +
            "               },\n" +
            "               {\n" +
            "                   \"@name\": \"connectionTimeout\",\n" +
            "                   \"$\": \"20000\"\n" +
            "               }\n" +
            "           ]\n" +
            "       }\n" +
            "   }\n" +
            "}";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JSONToAttributes.class);
    }

    @After
    public void tearDown() throws Exception {
        testRunner.shutdown();
    }

    @Test
    public void verifyThatHappyPathJsonToAttributesFlowsToSuccessRelationship() throws Exception {
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

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);
    }

    @Test(expected = AssertionError.class)
    public void makeSureFlattenAttributeSeparatorCausesErrorWhenLongerThanACharacter() throws Exception {
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON_SEPARATOR, "asd");

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
    }

    @Test
    public void makeSureAttributesAreProperlyFlattened() throws Exception {
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON, "true");
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON_ARRAYS, "true");
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON_SEPARATOR, "~");

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

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        MockFlowFile mf = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS).get(0);

        mf.assertAttributeEquals("name~first", "\"Donna\"");
        mf.assertAttributeEquals("name~last", "\"Hardin\"");
        mf.assertAttributeEquals("tags[0]", "\"culpa\"");
        mf.assertAttributeEquals("tags[1]", "\"sunt\"");
        mf.assertAttributeEquals("tags[2]", "\"reprehenderit\"");
        mf.assertAttributeEquals("tags[3]", "\"fugiat\"");
    }

    @Test
    public void makeSureFlatteningJSONWorksAsExpectedWithComplexNestingWithArraysDisabled() throws Exception {
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON, "true");
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(JSON_TEST.getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        MockFlowFile mf = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS).get(0);

        mf.assertAttributeEquals("Port.@alias", "defaultHttp");
        mf.assertAttributeEquals("Port.Enabled", "true");
        mf.assertAttributeEquals("Port.Number", "10092");
        mf.assertAttributeEquals("Port.Protocol", "http");
        mf.assertAttributeEquals("Port.KeepAliveTimeout", "20000");
        mf.assertAttributeEquals("Port.ThreadPool.@enabled", "false");
        mf.assertAttributeEquals("Port.ThreadPool.Max", "150");
        mf.assertAttributeEquals("Port.ThreadPool.ThreadPriority", "5");
        mf.assertAttributeEquals("Port.ExtendedProperties.Property", "[{\"@name\":\"connectionTimeout\",\"$\":\"20000\"},{\"@name\":\"connectionTimeout\",\"$\":\"20000\"}]");
    }

    @Test
    public void makeSureFlatteningJSONWorksAsExpectedWithComplexNesting() throws Exception {
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON, "true");
        testRunner.setProperty(JSONToAttributes.FLATTEN_JSON_ARRAYS, "true");
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(JSON_TEST.getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        MockFlowFile mf = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS).get(0);

        mf.assertAttributeEquals("Port.@alias", "defaultHttp");
        mf.assertAttributeEquals("Port.Enabled", "true");
        mf.assertAttributeEquals("Port.Number", "10092");
        mf.assertAttributeEquals("Port.Protocol", "http");
        mf.assertAttributeEquals("Port.KeepAliveTimeout", "20000");
        mf.assertAttributeEquals("Port.ThreadPool.@enabled", "false");
        mf.assertAttributeEquals("Port.ThreadPool.Max", "150");
        mf.assertAttributeEquals("Port.ThreadPool.ThreadPriority", "5");
        mf.assertAttributeEquals("Port.ExtendedProperties.Property[0].@name", "connectionTimeout");
        mf.assertAttributeEquals("Port.ExtendedProperties.Property[0].$", "20000");
        mf.assertAttributeEquals("Port.ExtendedProperties.Property[1].@name", "connectionTimeout");
        mf.assertAttributeEquals("Port.ExtendedProperties.Property[1].$", "20000");
    }

    @Test
    public void verifyMalformedJsonGoesToFailureRelationship() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{malformed".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_FAILURE);
    }

    @Test
    public void makeSureAllTopLevelKeysGetCreatedAsAttributesAndTheirValuesAreAsExpected() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"k1\":\"v1\",\"k2\":[],\"k3\":{},\"k4\":\"{}\"}".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            assertEquals("\"v1\"", file.getAttribute("k1"));
            assertEquals("[]", file.getAttribute("k2"));
            assertEquals("{}", file.getAttribute("k3"));
            assertEquals("\"{}\"", file.getAttribute("k4"));
        }
    }

    @Test
    public void makeSureGetRelationshipOnlyContainsProperRelationships() throws Exception {
        Set<Relationship> relationships = new JSONToAttributes().getRelationships();
        assertTrue(relationships.contains(JSONToAttributes.REL_FAILURE));
        assertTrue(relationships.contains(JSONToAttributes.REL_SUCCESS));
        assertTrue(relationships.contains(JSONToAttributes.REL_NO_CONTENT));
        assertEquals(3, relationships.size());
    }

    @Test
    public void makeSurePropertyDescriptorsAreProperlySetup() throws Exception {
        List<PropertyDescriptor> supportedPropertyDescriptors = new JSONToAttributes().getSupportedPropertyDescriptors();
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.OVERRIDE_ATTRIBUTES));
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.PRESERVE_TYPE));
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.JSON_ATTRIBUTE_NAME));
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.FLATTEN_JSON));
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.FLATTEN_JSON_ARRAYS));
        assertTrue(supportedPropertyDescriptors.contains(JSONToAttributes.FLATTEN_JSON_SEPARATOR));
        assertEquals(6, supportedPropertyDescriptors.size());
    }

    @Test
    public void verifyJsonWithOverlappingAttributesDoesNotOverrideWhenOverrideAttributesPropertyIsFalse() throws Exception {
        testRunner.setProperty(JSONToAttributes.OVERRIDE_ATTRIBUTES, "false");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"test\":\"somethingelse\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "test", "something");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            assertEquals("something", file.getAttribute("test"));
        }
    }

    @Test
    public void verifyJsonProperlySavesAttributesWhenPreserveTypeIsFalse() throws Exception {
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"test\":\"somethingelse\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "test", "something");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            assertEquals("somethingelse", file.getAttribute("test"));
        }
    }

    @Test
    public void verifyJsonProperlySavesAttributesWhenReadingFromAttribute() throws Exception {
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");
        testRunner.setProperty(JSONToAttributes.JSON_ATTRIBUTE_NAME, "test");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"test\":\"something\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "test", "{\"test\":\"somethingelse\"}");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            assertEquals("somethingelse", file.getAttribute("test"));
        }
    }

    @Test
    public void emptyAttributeShouldProperlyFlowThrough() throws Exception {
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");
        testRunner.setProperty(JSONToAttributes.JSON_ATTRIBUTE_NAME, "test");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"test\":\"something\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "test", "");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_NO_CONTENT);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_NO_CONTENT);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile file : flowFiles) {
            assertEquals("", file.getAttribute("test"));
        }
    }

    @Test
    public void emptyContentShouldProperlyFlowThrough() throws Exception {
        testRunner.setProperty(JSONToAttributes.PRESERVE_TYPE, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_NO_CONTENT);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_NO_CONTENT);

        assertEquals(1, flowFiles.size());
    }

    @Test
    public void definedJsonAttributeNameAndNoAttributeDefinedShouldFail() throws Exception {
        testRunner.setProperty(JSONToAttributes.JSON_ATTRIBUTE_NAME, "JSON_TEST.attribute");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_FAILURE);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_FAILURE);

        assertEquals(1, flowFiles.size());
    }

    @Test
    public void definedJsonAttributeNameAndEmptyAttributeValueShouldRouteToNoContent() throws Exception {
        testRunner.setProperty(JSONToAttributes.JSON_ATTRIBUTE_NAME, "JSON_TEST.attribute");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.putAttribute(flowFile, "JSON_TEST.attribute", "");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_NO_CONTENT);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_NO_CONTENT);

        assertEquals(1, flowFiles.size());
    }

    @Test
    public void noDefinedJsonAttributeNameAndEmptyContentShouldRouteToNoContent() throws Exception {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_NO_CONTENT);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_NO_CONTENT);

        assertEquals(1, flowFiles.size());
    }

    @Test
    public void verifyJsonDoesNotOverrideUuidAttribute() throws Exception {
        testRunner.setProperty(JSONToAttributes.OVERRIDE_ATTRIBUTES, "true");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"uuid\":\"somethingelse\"}".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONToAttributes.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONToAttributes.REL_SUCCESS);

        assertEquals(1, flowFiles.size());
    }
}
