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
package ru.kireev.nifi.processors.timestamp2date;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"timestamp", "date", "custom", "kireev"})
@CapabilityDescription("The processor accepts a list of attributes separated by comma and converts their values" +
        " formatted as (Date(1644417760) to the date formatted as 2022-02-09")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class Timestamp2DateProcessor extends AbstractProcessor {
    private static final String AT_LIST_SEPARATOR = ",";

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be formatted as the date.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to the date format").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        properties = new ArrayList<>();
        properties.add(ATTRIBUTES_LIST);
        properties = Collections.unmodifiableList(properties);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    private Set<String> parseAttributes(String atrList) {
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = atrList.split(AT_LIST_SEPARATOR);

            if (ats.length > 0) {
                Set<String> result = new HashSet<>(ats.length);

                for (String str : ats) {
                    String trim = str.trim();
                    result.add(trim);
                }
                return result;
            }
        }
        return null;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();

        if ( flowFile != null ) {
            Set<String> parsedAttributes = parseAttributes(context.getProperty(ATTRIBUTES_LIST).getValue());

            if (parsedAttributes != null) {
                for (String attribute : parsedAttributes) {
                    String attributeValue = flowFile.getAttribute(attribute);

                    if (!attributeValue.isEmpty() && attributeValue.contains("/Date(")) {
                        int x1 = attributeValue.indexOf('(');
                        int x2 = attributeValue.indexOf(')');

                        if (x1 >= 0 && x2 > x1) {
                            String timestamp = attributeValue.substring(x1 + 1, x2);

                            try {
                                long tsAsLong = Long.parseLong(timestamp);
                                Timestamp stamp = new Timestamp(tsAsLong);

                                String result = stamp.toLocalDateTime().toString().split("T")[0];
                                flowFile = session.putAttribute(flowFile, attribute, result);


                            } catch (Exception ignored) {

                            }

                        }
                    }

                }

                session.transfer(flowFile, REL_SUCCESS);
            }

        }
    }
}
