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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Timestamp2DateProcessorTest {
    private TestRunner testRunner;
    private final String contents = "Test contents";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Timestamp2DateProcessor.class);
    }

    @Test
    public void testProcessor() {
        testAttributesGivenOrNot();
        testOneAttributeGiven();
        testBrokenAttributesGiven();
        testTwoAttributesGiven();
    }

    @Test
    public void testAttributesGivenOrNot() {
        init();

        // Attributes list cannot be empty

        final String noAttributesGiven = "";
        final String someAttributesGiven = "some";

        // Trying to put an empty string
        testRunner.setProperty(Timestamp2DateProcessor.ATTRIBUTES_LIST, noAttributesGiven);
        testRunner.assertNotValid();

        // Trying to put a correct string
        testRunner.setProperty(Timestamp2DateProcessor.ATTRIBUTES_LIST, someAttributesGiven);
        testRunner.assertValid();
    }

    @Test
    public void testOneAttributeGiven() {
        withOneAttributeGiven("a.test1", "/Date(1644364800000)/", "2022-02-09");
    }

    @Test
    public void testTwoAttributesGiven() {
        init();

        final String attrName = "a.test1";
        final String attrValue = "/Date(1644364800000)/";
        final String attrExpectedValue = "2022-02-09";

        final String attrName2 = "a.test2";
        final String attrValue2 = "/Date(379179912000)/";
        final String attrExpectedValue2 = "1982-01-06";

        testRunner.setProperty(Timestamp2DateProcessor.ATTRIBUTES_LIST, String.join(",", attrName, attrName2));

        // Generating attributes
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(attrName, attrValue);
        attributes.put(attrName2, attrValue2);

        // Put contents and attributes to the flowfile
        testRunner.enqueue(contents, attributes);

        // Run processor false, false = without @OnUnSchedule and @OnSchedule just with @OnTrigger
        testRunner.run(1, false, false);

        // After processing there should not be the queue
        testRunner.assertQueueEmpty();

        // Get the resulting flowfile
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Timestamp2DateProcessor.REL_SUCCESS);

        // The amount of flowfiles at the exit should be exactly one
        MockFlowFile result = results.get(0);

        // the attribute's value should be processed
        result.assertAttributeEquals(attrName, attrExpectedValue);
        result.assertAttributeEquals(attrName2, attrExpectedValue2);

        // the contents should not be touched
        result.assertContentEquals(contents);

    }

    @Test
    public void testBrokenAttributesGiven() {
        Map<String, String[]> attributes = new HashMap<>();

        // key is the attribute's name, the first element is the value, the second element is an expecting result
        attributes.put("a.test1", new String[] {"/Date(16443648a00000)/", "/Date(16443648a00000)/"});
        attributes.put("a.test2", new String[] {"/Date(16443648 00000)/", "/Date(16443648 00000)/"});
        attributes.put("a.test3", new String[] {"/Date(   )/", "/Date(   )/"});
        attributes.put("a.test4", new String[] {"/Date(1644(3648))/", "/Date(1644(3648))/"});


        for (String attrName : attributes.keySet()) {
            withOneAttributeGiven(attrName, attributes.get(attrName)[0], attributes.get(attrName)[1]);
        }
    }


    private void withOneAttributeGiven(String attrName, String attrValue, String attrExpectedValue) {
        init();
        testRunner.setProperty(Timestamp2DateProcessor.ATTRIBUTES_LIST, attrName);

        // Generating attributes
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(attrName, attrValue);

        // Put contents and attributes to the flowfile
        testRunner.enqueue(contents, attributes);

        // Run processor false, false = without @OnUnSchedule and @OnSchedule just with @OnTrigger
        testRunner.run(1, false, false);

        // After processing there should not be the queue
        testRunner.assertQueueEmpty();

        // Get the resulting flowfile
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Timestamp2DateProcessor.REL_SUCCESS);

        // The amount of flowfiles at the exit should be exactly one
        MockFlowFile result = results.get(0);

        // the attribute's value should be processed
        result.assertAttributeEquals(attrName, attrExpectedValue);

        // the contents should not be touched
        result.assertContentEquals(contents);

    }
}
