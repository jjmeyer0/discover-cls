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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor will parse a JSON document and extract all first level keys from the JSON object. It will put all these attributes to 'attribute-list'.")
@WritesAttributes({
        @WritesAttribute(attribute = "attribute-list", description = "This attribute will contain all first level keys separated by the define separator.")
})
@ReadsAttributes({
        @ReadsAttribute(attribute = "X", description = "X is defined in JSON Attribute Name. It is not required. If it is then this processor will read this attribute's value to create " +
                "the attribute list.")
})
@SeeAlso({AttributesToTypedJSON.class, JSONToAttributes.class})
public class JSONKeysToAttributeList extends AbstractProcessor {
    static final String ATTRIBUTE_LIST_ATTRIBUTE = "attribute-list";

    static final PropertyDescriptor ATTRIBUTE_LIST_SEPARATOR = new PropertyDescriptor.Builder()
            .name("Attribute List Separator")
            .displayName("Attribute List Separator")
            .description("The separator that will be used separate the keys of the parsed JSON.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(",")
            .build();

    static final PropertyDescriptor JSON_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("JSON Attribute Name")
            .displayName("JSON Attribute Name")
            .description("If this value is populated then this processor will read JSON from attribute named in this property. For example, if this property is X, it will try to read " +
                    "JSON from a flow file's X attribute instead of its content.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted JSON to attributes.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed converting JSON to attributes.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(ATTRIBUTE_LIST_SEPARATOR, JSON_ATTRIBUTE));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String attributeName = context.getProperty(JSON_ATTRIBUTE).evaluateAttributeExpressions().getValue();
        final String attributeContent = flowFile.getAttribute(attributeName);
        final byte[] content = attributeContent == null ? FlowFileUtils.extractMessage(flowFile, session) : attributeContent.getBytes();
        final String separator = context.getProperty(ATTRIBUTE_LIST_SEPARATOR).getValue();

        try {
            if (content == null || Arrays.equals(content, new byte[0])) {
                flowFile = session.putAttribute(flowFile, ATTRIBUTE_LIST_ATTRIBUTE, "");
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(content);
                final String attributeList = createAttributeList(jsonNode, separator);

                flowFile = session.putAttribute(flowFile, ATTRIBUTE_LIST_ATTRIBUTE, attributeList);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException e) {
            getLogger().error("Failed parsing JSON.", new Object[]{e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String createAttributeList(JsonNode jsonNode, String separator) {
        final List<String> jsonKeys = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> current = fields.next();
            jsonKeys.add(current.getKey());
        }

        boolean isFirst = true;
        String attributeList = "";
        for (String key : jsonKeys) {
            attributeList += isFirst ? key : separator + key;
            isFirst = false;
        }

        return attributeList;
    }
}
