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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.util.*;


@Tags({"json", "attributes", "flowfile"})
@CapabilityDescription("This processor will take a JSON string and convert each of the top level keys to attributes. Each attribute value " +
        "will be a valid JSON. For example, if a JSON string, '{\"key1\":\"val1\",\"key2\":0}', is converted to attributes, the following " +
        "attributes and their values would be created: key1 with value \"val1\" and key2 with value 0.")
@SeeAlso({AttributesToNestedJSON.class})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class JsonToAttributes extends AbstractProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted JSON to attributes.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed converting JSON to attributes.")
            .build();


    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList());

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

        final byte[] content = extractMessage(flowFile, session);

        try {
            final JsonNode jsonNode = OBJECT_MAPPER.readTree(content);
            final Map<String, String> attributes = new HashMap<>();
            jsonNode.fields().forEachRemaining(entry -> {
                switch (entry.getValue().getNodeType()) {
                    case ARRAY:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case BINARY:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case BOOLEAN:
                        attributes.put(entry.getKey(), Boolean.toString(entry.getValue().asBoolean()));
                        break;
                    case MISSING:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case NULL:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case NUMBER:
                        attributes.put(entry.getKey(), Long.toString(entry.getValue().asLong()));
                        break;
                    case OBJECT:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case POJO:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                    case STRING:
                        attributes.put(entry.getKey(), entry.getValue().toString());
                        break;
                }
            });

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (IOException e) {
            getLogger().error("Failed parsing JSON.", new Object[]{e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessage(FlowFile flowFile, ProcessSession session) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, messageContent, true));
        return messageContent;
    }
}
