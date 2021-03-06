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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a JSON representation of the input FlowFile Attributes. The resulting JSON " +
        "can be written to either as a new attribute, 'JSONAttributes', or written to the flow file's content. This processor " +
        "will try and keep the attribute values' types as best it can. If parsing of an attribute fails, the value will automatically be stored " +
        "as a string. If you have an attribute with a value 200, this processor will create a json where the said attribute's value is 200, a number not a string.")
@WritesAttribute(attribute = "JSONAttributes", description = "JSON representation of Attributes")
@SeeAlso({AttributesToTypedJSON.class})
public class AttributesToTypedJSON extends AbstractProcessor {

    static final String JSON_ATTRIBUTE_NAME = "JSONAttributes";
    static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    static final String DESTINATION_CONTENT = "flowfile-content";
    static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON. If this value " +
                    "is left empty then all existing Attributes will be included. This list of attributes is " +
                    "case sensitive. If an attribute specified in the list is not found it will be be emitted " +
                    "to the resulting JSON with an empty string or NULL value.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Used to control if JSON value is written as a new flow file attribute '" + JSON_ATTRIBUTE_NAME + "' " +
                    "or written in the flow file content. Writing to flow file content will overwrite any " +
                    "existing flow file content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();
    static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
                    "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name("Null Value")
            .description("If true, a non-existing or empty attribute will be NULL in the resulting JSON. If false an empty " +
                    "string will be placed in the JSON")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to JSON").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();
    private static final String AT_LIST_SEPARATOR = ",";
    private static final String APPLICATION_JSON = "application/json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(ATTRIBUTES_LIST, DESTINATION, INCLUDE_CORE_ATTRIBUTES, NULL_VALUE_FOR_EMPTY_STRING));
    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return Map of values that are feed to a Jackson ObjectMapper
     */
    private Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, String atrList,
                                                              boolean includeCoreAttributes,
                                                              boolean nullValForEmptyString) {

        Map<String, String> atsToWrite = new LinkedHashMap<>();

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        if (nullValForEmptyString) {
                            atsToWrite.put(cleanStr, null);
                        } else {
                            atsToWrite.put(cleanStr, "");
                        }
                    }
                }
            }
        } else {
            atsToWrite.putAll(ff.getAttributes());
        }

        if (!includeCoreAttributes) {
            atsToWrite = removeCoreAttributes(atsToWrite);
        }

        return atsToWrite;
    }

    /**
     * Remove all of the CoreAttributes from the Attributes that will be written to the Flowfile.
     *
     * @param atsToWrite List of Attributes that have already been generated including the CoreAttributes
     * @return Difference of all attributes minus the CoreAttributes
     */
    private Map<String, String> removeCoreAttributes(Map<String, String> atsToWrite) {
        for (CoreAttributes c : CoreAttributes.values()) {
            atsToWrite.remove(c.key());
        }
        return atsToWrite;
    }

    private boolean isCoreAttribute(Map.Entry<String, String> attribute) {
        for (CoreAttributes ca : CoreAttributes.values()) {
            if (ca.key().equals(attribute.getKey())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final Map<String, String> atrList = buildAttributesMapForFlowFile(original,
                context.getProperty(ATTRIBUTES_LIST).evaluateAttributeExpressions(original).getValue(),
                context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean(),
                context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean());

        final Map<String, Object> typedList = new LinkedHashMap<>();

        try {
            for (Map.Entry<String, String> attribute : atrList.entrySet()) {
                if (isCoreAttribute(attribute)) {
                    typedList.put(attribute.getKey(), attribute.getValue());
                } else {
                    try {
                        if (attribute.getValue() != null) {
                            JsonNode node = OBJECT_MAPPER.readTree(attribute.getValue().getBytes());
                            Object o = OBJECT_MAPPER.treeToValue(node, Object.class);
                            typedList.put(attribute.getKey(), o);
                        } else {
                            typedList.put(attribute.getKey(), null);
                        }
                    } catch (JsonProcessingException e) {
                        // Any JSON that can't be parsed is stored as a string.
                        typedList.put(attribute.getKey(), attribute.getValue());
                    }
                }
            }

            switch (context.getProperty(DESTINATION).getValue()) {
                case DESTINATION_ATTRIBUTE:
                    FlowFile atFlowfile = session.putAttribute(original, JSON_ATTRIBUTE_NAME,
                            OBJECT_MAPPER.writeValueAsString(typedList));
                    session.transfer(atFlowfile, REL_SUCCESS);
                    break;
                case DESTINATION_CONTENT:
                    FlowFile conFlowFile = session.write(original, new StreamCallback() {
                        @Override
                        public void process(InputStream in, OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(OBJECT_MAPPER.writeValueAsBytes(typedList));
                            }
                        }
                    });
                    conFlowFile = session.putAttribute(conFlowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                    session.transfer(conFlowFile, REL_SUCCESS);
                    break;
            }
        } catch (IOException e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
