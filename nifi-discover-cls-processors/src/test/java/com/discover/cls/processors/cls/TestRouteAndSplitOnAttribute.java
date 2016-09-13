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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestRouteAndSplitOnAttribute {
    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(RouteAndSplitOnAttribute.class);
    }

    @Test
    public void happyPathTest() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a_ip,b,c_ip,d_ip,e");
        testRunner.setProperty("matched.ips", ".*_ip");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "a_ip", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v3");
        flowFile = session.putAttribute(flowFile, "d_ip", "v4");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("matched.ips");

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship("matched.ips");

        assertEquals(3, flowFiles.size());

        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("a_ip", "v1");
            mockFlowFile.assertAttributeEquals("c_ip", "v3");
            mockFlowFile.assertAttributeEquals("k2", "v2");
            mockFlowFile.assertAttributeEquals("d_ip", "v4");
        }
    }

    @Test
    public void makeSureDeletingAttributesProperlyWorks() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a_ip,b1,c_ip,d_ip,e");
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTES_TO_KEEP, "k2, k3, k4");
        testRunner.setProperty("matched.ips", ".*_ip");
        testRunner.setProperty("matched.other", ".*1");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "k1", "v1");
        flowFile = session.putAttribute(flowFile, "a_ip", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v2");
        flowFile = session.putAttribute(flowFile, "d_ip", "v2");
        flowFile = session.putAttribute(flowFile, "b1", "b1");
        flowFile = session.putAttribute(flowFile, "k3", "v3");
        flowFile = session.putAttribute(flowFile, "k4", "v4");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertTransferCount("matched.ips", 3);
        testRunner.assertTransferCount("matched.other", 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship("matched.ips");

        assertEquals(3, flowFiles.size());

        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("k1", null);
            mockFlowFile.assertAttributeEquals("k3", "v3");
            mockFlowFile.assertAttributeEquals("k4", "v4");

            if (mockFlowFile.getAttribute(RouteAndSplitOnAttribute.ROUTE_ATTRIBUTE_MATCHED_KEY).equals("a_ip")) {
                mockFlowFile.assertAttributeEquals(RouteAndSplitOnAttribute.ROUTE_ATTRIBUTE_MATCHED_VALUE, "v2");
            }

        }
    }

    @Test
    public void makeSureRouteAllMatchValueProperlyTransfersToMatchWhenAllMatch() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_ALL_MATCH);
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a_ip,b,c_ip,d_ip,e");
        testRunner.setProperty("matched.ips", ".*_ip");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "a_ip", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v3");
        flowFile = session.putAttribute(flowFile, "d_ip", "v4");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RouteAndSplitOnAttribute.REL_MATCH);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(RouteAndSplitOnAttribute.REL_MATCH);

        assertEquals(3, flowFiles.size());

        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("a_ip", "v1");
            mockFlowFile.assertAttributeEquals("k2", "v2");
            mockFlowFile.assertAttributeEquals("c_ip", "v3");
            mockFlowFile.assertAttributeEquals("d_ip", "v4");
        }
    }

    @Test
    public void makeSureRouteAllMatchValueProperlyTransfersToNoMatchWhenAllMatch() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_ALL_MATCH);
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "b,e,a_ip");
        testRunner.setProperty("matched.ips", ".*b");
        testRunner.setProperty("matched.other", ".*_ip");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "b", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "k3", "v3");
        flowFile = session.putAttribute(flowFile, "k4", "v4");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RouteAndSplitOnAttribute.REL_NO_MATCH);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(RouteAndSplitOnAttribute.REL_NO_MATCH);

        assertEquals(1, flowFiles.size());

        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("b", "v1");
            mockFlowFile.assertAttributeEquals("k2", "v2");
            mockFlowFile.assertAttributeEquals("k3", "v3");
            mockFlowFile.assertAttributeEquals("k4", "v4");
        }
    }

    @Test
    public void routeAnyMatchingValuesShouldRouteAllThatMatchToMatchedAndOthersToNotMatched() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_ANY_MATCHES);
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a_ip,c_ip,d_ip,e");
        testRunner.setProperty("matched.ips", ".*_ip");
        testRunner.setProperty("matched.other", "test");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "a_ip", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v3");
        flowFile = session.putAttribute(flowFile, "test", "v3");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_MATCH, 2);
        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_NO_MATCH, 0);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(RouteAndSplitOnAttribute.REL_MATCH);
        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("a_ip", "v1");
            mockFlowFile.assertAttributeEquals("c_ip", "v3");
            mockFlowFile.assertAttributeEquals("k2", "v2");
        }

    }

    @Test
    public void makeSureOnlyOneAttributeToMatchWorks() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_ANY_MATCHES);
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a_ip");
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTES_TO_KEEP, "k2");
        testRunner.setProperty("matched.ips", ".*_ip");
        testRunner.setProperty("matched.other", "test");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "a_ip", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v3");
        flowFile = session.putAttribute(flowFile, "test", "v3");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_MATCH, 1);
        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_NO_MATCH, 0);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(RouteAndSplitOnAttribute.REL_MATCH);
        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("a_ip", "v1");
            mockFlowFile.assertAttributeEquals("c_ip", null);
            mockFlowFile.assertAttributeEquals("k2", "v2");
        }

    }

    @Test
    public void shouldProperlyRouteOnlyOnesThatMatchEvenWhenOneRelationshipsMatcherDoesntMatchAny() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_ANY_MATCHES);
        testRunner.setProperty("match.ip", ".*_ip");
        testRunner.setProperty("test", "test");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "a_ip", "v1");
        flowFile = session.putAttribute(flowFile, "k2", "v2");
        flowFile = session.putAttribute(flowFile, "c_ip", "v3");
        flowFile = session.putAttribute(flowFile, "k3", "v3");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_MATCH, 2);
        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_NO_MATCH, 0);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(RouteAndSplitOnAttribute.REL_MATCH);
        for (MockFlowFile mockFlowFile : flowFiles) {
            mockFlowFile.assertAttributeEquals("a_ip", "v1");
            mockFlowFile.assertAttributeEquals("c_ip", "v3");
            mockFlowFile.assertAttributeEquals("k2", "v2");
        }

    }

    @Test
    public void makeSureAllRouteToUnmatchedWwhenAttributesToMatchDontExist() throws Exception {
        testRunner.setProperty(RouteAndSplitOnAttribute.ROUTE_STRATEGY, RouteAndSplitOnAttribute.ROUTE_PROPERTY_NAME);
        testRunner.setProperty(RouteAndSplitOnAttribute.ATTRIBUTE_LIST_TO_MATCH, "a,k2,c,k3");
        testRunner.setProperty("match.ip", ".*_ip");
        testRunner.setProperty("match.ipp", ".*_ip");
        testRunner.setProperty("test", "test");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, "b", "v1");
        flowFile = session.putAttribute(flowFile, "k", "v2");
        flowFile = session.putAttribute(flowFile, "c", "v3");
        flowFile = session.putAttribute(flowFile, "k", "v3");

        testRunner.enqueue(flowFile);
        testRunner.run();

        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_MATCH, 0);
        testRunner.assertTransferCount(RouteAndSplitOnAttribute.REL_NO_MATCH, 1);

    }
}
