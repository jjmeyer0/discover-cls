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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor will take a JSON string from either a define attribute or the flow file's content. It will convert each of the top level keys to attributes flow file " +
        "attributes. This processor will read the JSON from the attribute defined in property descriptor named 'JSON Attribute Name.' If this is not defined then the content will be read. " +
        "Be aware if if 'JSON Attribute Name' has a value then the content of the flow file will not be looked at. If 'Preserverve JSON Type' is true then each attribute value created will " +
        "be a valid JSON. For example, if a JSON string, '{\"key1\":\"val1\",\"key2\":0}', is converted to attributes, the following attributes and their values would be created: key1 with " +
        "value \"val1\" and key2 with value 0. Notice the quotes. This is done because each of the values generated from this processor must be valid JSON. However, this functionality is " +
        "configurable. This is the default behavior. If this is not desired then switch the property 'Preserve JSON Type' to false. This will then output string values without the outer " +
        "double quotes.")
@SeeAlso({AttributesToTypedJSON.class, JSONToAttributes.class, JSONKeysToAttributeList.class})
@ReadsAttributes({
        @ReadsAttribute(attribute = "X", description = "The name of the attribute to get the JSON data from. This is optional. X is defined in property descriptor JSON Attribute Name.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "Y", description = "Y is the value populated in 'JSON Attribute Name'. If it is populated the processor will read JSON from an attribute with the name " +
                "in 'JSON Attribute Name'.")
})
public class JSONToAttributes extends AbstractProcessor {
    static final PropertyDescriptor OVERRIDE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Override Attributes")
            .displayName("Override Attributes")
            .description("If true a JSON key with the same name as a flow file attribute will overwrite the value otherwise the value will be dropped.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    static final PropertyDescriptor PRESERVE_TYPE = new PropertyDescriptor.Builder()
            .name("Preserve JSON Type")
            .displayName("Preserve JSON Type")
            .description("Determines whether or not this processor should try and preserve the JSON value types when writing them to attribute values.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    static final PropertyDescriptor JSON_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("JSON Attribute Name")
            .displayName("JSON Attribute Name")
            .description("If this value is populated then this processor will read JSON from attribute instead of content.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted JSON to attributes.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed converting JSON to attributes.")
            .build();

    static final Relationship REL_NO_CONTENT = new Relationship.Builder()
            .name("no content")
            .description("The define attribute and the flow file content does not contain data.")
            .build();


    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_NO_CONTENT)));
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(OVERRIDE_ATTRIBUTES, PRESERVE_TYPE, JSON_ATTRIBUTE_NAME));

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

        final String attributeNameFromProperty = context.getProperty(JSON_ATTRIBUTE_NAME).evaluateAttributeExpressions().getValue();
        final String attributeContent = flowFile.getAttribute(attributeNameFromProperty);
        final byte[] content = getContent(session, flowFile, attributeNameFromProperty, attributeContent);
        final boolean toOverride = context.getProperty(OVERRIDE_ATTRIBUTES).asBoolean();
        final boolean preserveType = context.getProperty(PRESERVE_TYPE).asBoolean();

        if (attributeNameFromProperty != null && !"".equals(attributeNameFromProperty) && attributeContent == null) {
            getLogger().error(JSON_ATTRIBUTE_NAME.getDisplayName() + " is defined, but the attribute '" + attributeNameFromProperty + "' does not exist.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (content == null || Arrays.equals(content, new byte[0])) {
            // No content. push through.
            getLogger().debug("No content is defined. Passing flow file to 'no content' relationship.");
            session.transfer(flowFile, REL_NO_CONTENT);
            return;
        }

        try {
            final JsonNode jsonNode = OBJECT_MAPPER.readTree(content);
            final Map<String, String> attributes = new LinkedHashMap<>();
            final Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                if (toOverride || flowFile.getAttribute(entry.getKey()) == null) {
                    JsonNode value = entry.getValue();
                    switch (value.getNodeType()) {
                        case ARRAY:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case BINARY:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case BOOLEAN:
                            attributes.put(entry.getKey(), Boolean.toString(value.asBoolean()));
                            break;
                        case MISSING:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case NULL:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case NUMBER:
                            attributes.put(entry.getKey(), Long.toString(value.asLong()));
                            break;
                        case OBJECT:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case POJO:
                            attributes.put(entry.getKey(), value.toString());
                            break;
                        case STRING:
                            attributes.put(entry.getKey(), preserveType ? value.toString() : value.textValue());
                            break;
                    }
                }
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (IOException e) {
            getLogger().error("Failed parsing JSON.", new Object[]{e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private byte[] getContent(ProcessSession session, FlowFile flowFile, String attributeNameFromProperty, String attributeContent) {
        if (attributeNameFromProperty == null || "".equals(attributeNameFromProperty)) {
            return FlowFileUtils.extractMessage(flowFile, session);
        } else {
            return attributeContent == null ? new byte[0] : attributeContent.getBytes();
        }
    }
}
