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


public class TestJSONKeysToAttributeList {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JSONKeysToAttributeList.class);
    }

    @After
    public void tearDown() throws Exception {
        testRunner.shutdown();
    }

    @Test(expected = AssertionError.class)
    public void attributeListMustBeDefined() {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        testRunner.enqueue(flowFile);
        testRunner.run();
    }

    @Test
    public void processorShouldProperlyConvertSimpleJsonToAttributes() throws Exception {
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

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONKeysToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("key1,key2", file.getAttribute(JSONKeysToAttributeList.ATTRIBUTE_LIST_ATTRIBUTE));
        }
    }

    @Test
    public void processorShouldProperlyConvertComplexJsonToAttributes() throws Exception {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "|");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":{\"k1\":\"v1\"},\"key2\":[\"val2\"],\"key3\":\"val3\"}".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONKeysToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("key1|key2|key3", file.getAttribute(JSONKeysToAttributeList.ATTRIBUTE_LIST_ATTRIBUTE));
        }
    }

    @Test
    public void makeSureGetRelationshipOnlyContainsProperRelationships() throws Exception {
        Set<Relationship> relationships = new JSONKeysToAttributeList().getRelationships();
        assertTrue(relationships.contains(JSONKeysToAttributeList.REL_FAILURE));
        assertTrue(relationships.contains(JSONKeysToAttributeList.REL_SUCCESS));
        assertEquals(2, relationships.size());
    }

    @Test
    public void makeSurePropertyDescriptorsAreProperlySetup() throws Exception {
        List<PropertyDescriptor> supportedPropertyDescriptors = new JSONKeysToAttributeList().getSupportedPropertyDescriptors();
        assertTrue(supportedPropertyDescriptors.contains(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR));
        assertTrue(supportedPropertyDescriptors.contains(JSONKeysToAttributeList.JSON_ATTRIBUTE));
        assertEquals(2, supportedPropertyDescriptors.size());
    }

    @Test
    public void makeSureProcessorProperlyHandlesMalformedJson() throws Exception {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "|");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":\"value1\",\"key2\":\"val2\"".getBytes());
            }
        });

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_FAILURE);
    }

    @Test
    public void makeSureProcessorProperlyReadsFromAttributeInsteadOfContent() throws Exception {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "|");
        testRunner.setProperty(JSONKeysToAttributeList.JSON_ATTRIBUTE, "JSON_TEST-value");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":\"value1\",\"key2\":\"val2\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "JSON_TEST-value", "{\"k6\":\"v6\"}");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONKeysToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("k6", file.getAttribute(JSONKeysToAttributeList.ATTRIBUTE_LIST_ATTRIBUTE));
        }
    }

    @Test
    public void makeSureNothingIsReturnedIfTopLevelJsonIsAnArray() throws Exception {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "|");
        testRunner.setProperty(JSONKeysToAttributeList.JSON_ATTRIBUTE, "JSON_TEST-value");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":\"value1\",\"key2\":\"val2\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "JSON_TEST-value", "[\"k6\",\"v6\"]");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONKeysToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("", file.getAttribute(JSONKeysToAttributeList.ATTRIBUTE_LIST_ATTRIBUTE));
        }
    }

    @Test
    public void makeSureEmptyContentIsProperlyHandled() throws Exception {
        testRunner.setProperty(JSONKeysToAttributeList.ATTRIBUTE_LIST_SEPARATOR, "|");
        testRunner.setProperty(JSONKeysToAttributeList.JSON_ATTRIBUTE, "JSON_TEST-value");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("{\"key1\":\"value1\",\"key2\":\"val2\"}".getBytes());
            }
        });

        flowFile = session.putAttribute(flowFile, "JSON_TEST-value", "");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(JSONKeysToAttributeList.REL_SUCCESS);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(JSONKeysToAttributeList.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        for (FlowFile file : flowFiles) {
            assertEquals("", file.getAttribute(JSONKeysToAttributeList.ATTRIBUTE_LIST_ATTRIBUTE));
        }
    }
}
