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

import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * This processor routes a FlowFile based on its flow file attributes by using regular expressions. There are three routing strategies in this processor: Route to Property Name, Route to 'matched'
 * if all match, and Route to 'matched' if any matches. For the latter two only have two relationships 'matched' and 'unmatched.' The first, 'Route to Property Name' can have multiple user defined
 * relationships. These relationships are determined by the properties the user of this processor creates. This processor can potentially produce multiple flow files. For example, if a user
 * defines a custom property that uses a regular expression, for each attribute matched this processor will clone the flow file. If none of the attributes are matched then the flow file will
 * be routed to 'unmatched' relationship.
 * </p>
 *
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "routing", "route", "Attribute Expression Language", "regexp", "regex", "Regular Expression", "Expression Language"})
@CapabilityDescription("This processor will route flow files and filter attributes based on a few defined properties. It uses regular expressions to match against the attributes.")
@WritesAttributes({
        @WritesAttribute(attribute = "RouteAndSplitOnAttribute.Matched.Route", description = "The dynamic attribute that may be a relationship name."),
        @WritesAttribute(attribute = "RouteAndSplitOnAttribute.Matched.Attribute", description = "The attribute matched and routed on."),
        @WritesAttribute(attribute = "RouteAndSplitOnAttribute.Matched.Value", description = "The value of the attribute that was matched and routed on."),
        @WritesAttribute(attribute = "RouteAndSplitOnAttribute.AttributeFilter", description = "The value of the property 'Attributes to Keep.'"),
        @WritesAttribute(attribute = "original-uuid", description = "If a flow file is cloned, this is the UUID of the flow file that was cloned from."),
})
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Attribute Expression Language")
public class RouteAndSplitOnAttribute extends AbstractProcessor {
    static final String ROUTE_ATTRIBUTE_NAME = "RouteAndSplitOnAttribute.Matched.Route";
    static final String ROUTE_ATTRIBUTE_MATCHED_KEY = "RouteAndSplitOnAttribute.Matched.Attribute";
    static final String ROUTE_ATTRIBUTE_MATCHED_VALUE = "RouteAndSplitOnAttribute.Matched.Value";
    static final String ROUTE_ATTRIBUTE_FILTER = "RouteAndSplitOnAttribute.AttributeFilter";
    static final String ORIGINAL_UUID = "original.uuid";

    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    private static final String ROUTE_ALL_MATCH_VALUE = "Route to 'match' if all match";
    private static final String ROUTE_ANY_MATCHES_VALUE = "Route to 'match' if any matches";
    private static final String ROUTE_PROPERTY_NAME_VALUE = "Route to Property name";

    static final AllowableValue ROUTE_PROPERTY_NAME = new AllowableValue(ROUTE_PROPERTY_NAME_VALUE, "Route to Property name",
            "A copy of the FlowFile will be routed to each relationship whose corresponding expression evaluates to 'true'");
    static final AllowableValue ROUTE_ALL_MATCH = new AllowableValue(ROUTE_ALL_MATCH_VALUE, "Route to 'matched' if all match",
            "Requires that all user-defined expressions evaluate to 'true' for the FlowFile to be considered a match");
    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    static final AllowableValue ROUTE_ANY_MATCHES = new AllowableValue(ROUTE_ANY_MATCHES_VALUE,
            "Route to 'matched' if any matches",
            "Requires that at least one user-defined expression evaluate to 'true' for hte FlowFile to be considered a match");

    static final PropertyDescriptor ROUTE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Routing Strategy")
            .displayName("Routing Strategy")
            .description("Specifies how to determine which relationship to send flow files to.")
            .required(true)
            .allowableValues(ROUTE_PROPERTY_NAME, ROUTE_ALL_MATCH, ROUTE_ANY_MATCHES)
            .defaultValue(ROUTE_PROPERTY_NAME.getValue())
            .build();

    static final PropertyDescriptor ATTRIBUTE_LIST_TO_MATCH = new PropertyDescriptor.Builder()
            .name("Attribute List to Route")
            .displayName("Attribute List to Route")
            .description("A comma-separated list. The processor will match against this list of values. Each value matched will produce a flow file. For an attribute to be considered matched, it " +
                    "must exist in this list and on the flow file. If this list is empty all the attributes on the flow file will be looked at.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor ATTRIBUTES_TO_KEEP = new PropertyDescriptor.Builder()
            .name("Attributes to Keep")
            .displayName("Attributes to Keep")
            .description("A comma-separated list of values of the attribute names to pass through. If all attributes are wanted leave blank.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any user-defined expression will be routed here.")
            .build();

    static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles will be routed to 'match' if one or all Expressions match, depending on the configuration of the Routing Strategy property")
            .build();

    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private List<PropertyDescriptor> properties;
    private volatile String configuredRouteStrategy = ROUTE_STRATEGY.getDefaultValue();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();


    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> set = new HashSet<>();
        set.add(REL_NO_MATCH);
        relationships = new AtomicReference<>(set);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROUTE_STRATEGY);
        properties.add(ATTRIBUTE_LIST_TO_MATCH);
        properties.add(ATTRIBUTES_TO_KEEP);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(ROUTE_STRATEGY)) {
            configuredRouteStrategy = newValue;
        } else if (descriptor.equals(ATTRIBUTE_LIST_TO_MATCH) || descriptor.equals(ATTRIBUTES_TO_KEEP)) {
            // don't really want to do anything on these ones.
        } else {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }

            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        final String routeStrategy = configuredRouteStrategy;
        if (ROUTE_PROPERTY_NAME.equals(routeStrategy)) {
            for (final String propName : allDynamicProps) {
                newRelationships.add(new Relationship.Builder().name(propName).build());
            }
        } else {
            newRelationships.add(REL_MATCH);
        }

        newRelationships.add(REL_NO_MATCH);
        this.relationships.set(newRelationships);
    }

    /**
     * When this processor is scheduled, update the dynamic properties into the map
     * for quick access during each onTrigger call
     *
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", new Object[]{descriptor});
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        final Set<Relationship> matchedRelationships = getMatchedRelations(context, flowFile, propMap);

        switch (context.getProperty(ROUTE_STRATEGY).getValue()) {
            case ROUTE_ALL_MATCH_VALUE:
                if (matchedRelationships.size() == propMap.size()) {
                    for (final Map.Entry<FlowFile, Relationship> entry : getFlowFilesToSend(session, context, flowFile, propMap, matchedRelationships).entrySet()) {
                        getLogger().info("Cloned {} into {} and routing clone to relationship {}", new Object[]{flowFile, entry.getValue(), entry.getKey()});
                        session.getProvenanceReporter().route(entry.getKey(), REL_MATCH);
                        session.transfer(entry.getKey(), REL_MATCH);
                    }
                } else {
                    session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
                    session.transfer(flowFile, REL_NO_MATCH);
                }
                break;
            case ROUTE_ANY_MATCHES_VALUE:
                if (matchedRelationships.isEmpty()) {
                    session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
                    session.transfer(flowFile, REL_NO_MATCH);
                } else {
                    for (final Map.Entry<FlowFile, Relationship> entry : getFlowFilesToSend(session, context, flowFile, propMap, matchedRelationships).entrySet()) {
                        getLogger().info("Cloned {} into {} and routing clone to relationship {}", new Object[]{flowFile, entry.getValue(), entry.getKey()});
                        session.getProvenanceReporter().route(entry.getKey(), REL_MATCH);
                        session.transfer(entry.getKey(), REL_MATCH);
                    }
                }
                break;
            case ROUTE_PROPERTY_NAME_VALUE:
            default:
                if (matchedRelationships.isEmpty()) {
                    session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
                    session.transfer(flowFile, REL_NO_MATCH);
                } else {
                    for (final Map.Entry<FlowFile, Relationship> entry : getFlowFilesToSend(session, context, flowFile, propMap, matchedRelationships).entrySet()) {
                        getLogger().info("Cloned {} into {} and routing clone to relationship {}", new Object[]{flowFile, entry.getValue(), entry.getKey()});
                        session.getProvenanceReporter().route(entry.getKey(), entry.getValue());
                        session.transfer(entry.getKey(), entry.getValue());
                    }
                }
                break;
        }
    }

    private Map<FlowFile, Relationship> getFlowFilesToSend(ProcessSession session, ProcessContext context,
                                                           FlowFile flowFile, final Map<Relationship, PropertyValue> propMap,
                                                           final Set<Relationship> matchedRelationships) {
        final String attributesToKeepValue = context.getProperty(ATTRIBUTES_TO_KEEP).evaluateAttributeExpressions(flowFile).getValue();
        final Set<String> attributesToMatch = getAttributesToMatch(context, flowFile);
        final Set<String> attributesToKeep = getAttributesToKeep(context, flowFile);

        final Map<FlowFile, Relationship> flowFileRelationshipMap = new HashMap<>();
        int i = 0;
        for (final Relationship relationship : matchedRelationships) {
            final PropertyValue value = propMap.get(relationship);
            final String patternToMatchAgainst = value.evaluateAttributeExpressions(flowFile).getValue();
            final List<String> allAttributesToMatch = getMatchedAttributes(patternToMatchAgainst, attributesToMatch);

            for (int j = 0; j < allAttributesToMatch.size(); j++) {
                FlowFile f = i == matchedRelationships.size() - 1 && j == allAttributesToMatch.size() - 1 ? flowFile : session.clone(flowFile);

                final String attributeKey = allAttributesToMatch.get(j);
                final String attributeValue = f.getAttribute(attributeKey);

                for (Map.Entry<String, String> attribute : f.getAttributes().entrySet()) {
                    if (!attributesToKeep.isEmpty() && !attributesToKeep.contains(attribute.getKey())) {
                        f = session.removeAttribute(f, attribute.getKey());
                    }
                }

                f = session.putAttribute(f, ROUTE_ATTRIBUTE_NAME, relationship.getName());
                f = session.putAttribute(f, ROUTE_ATTRIBUTE_MATCHED_KEY, attributeKey);
                f = session.putAttribute(f, ROUTE_ATTRIBUTE_MATCHED_VALUE, attributeValue == null ? "" : attributeValue);
                f = session.putAttribute(f, ROUTE_ATTRIBUTE_FILTER, attributesToKeepValue == null ? "": attributesToKeepValue);
                f = session.putAttribute(f, attributeKey, attributeValue == null ? "" : attributeValue);
                f = session.putAttribute(f, ORIGINAL_UUID, flowFile.getAttribute(CoreAttributes.UUID.key()));

                flowFileRelationshipMap.put(f, relationship);
            }
            i++;
        }

        return flowFileRelationshipMap;
    }

    private List<String> getMatchedAttributes(String patternToMatchAgainst, Set<String> attributesToMatch) {
        List<String> allAttributesToMatch = new ArrayList<>();
        for (String attribute : attributesToMatch) {
            if (attribute.matches(patternToMatchAgainst)) {
                allAttributesToMatch.add(attribute);
            }
        }
        return allAttributesToMatch;
    }

    private Set<Relationship> getMatchedRelations(ProcessContext context, FlowFile flowFile, Map<Relationship, PropertyValue> propertyMap) {
        Set<Relationship> matchedPropertyMap = new HashSet<>();
        for (Map.Entry<Relationship, PropertyValue> entry : propertyMap.entrySet()) {
            final Set<String> attributesToMatch = getAttributesToMatch(context, flowFile);
            final String pattern = entry.getValue().evaluateAttributeExpressions().getValue();

            for (String attribute : attributesToMatch) {
                if (attribute.matches(pattern)) {
                    matchedPropertyMap.add(entry.getKey());
                }
            }
        }

        return matchedPropertyMap;
    }

    private Set<String> getAttributesToMatch(ProcessContext context, FlowFile flowFile) {
        final String value = context.getProperty(ATTRIBUTE_LIST_TO_MATCH).evaluateAttributeExpressions(flowFile).getValue();

        if (value == null || "".equals(value)) {
            return flowFile.getAttributes().keySet();
        } else {
            final Set<String> attributes = new HashSet<>();
            final String[] attributesToMatch = value.split(",");

            for (String attribute : attributesToMatch) {
                if (flowFile.getAttribute(attribute) != null) {
                    attributes.add(attribute);
                }
            }

            return attributes;
        }
    }

    private Set<String> getAttributesToKeep(ProcessContext context, FlowFile flowFile) {
        String property = context.getProperty(ATTRIBUTES_TO_KEEP).evaluateAttributeExpressions(flowFile).getValue();

        if ("".equals(property) || property == null) {
            return new HashSet<>();
        } else {
            List<String> cleanedValues = new ArrayList<>();

            for (String s : property.split(",")) {
                cleanedValues.add(s.trim());
            }
            return new HashSet<>(cleanedValues);
        }
    }
}
