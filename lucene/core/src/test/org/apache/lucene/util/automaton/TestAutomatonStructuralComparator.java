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
package org.apache.lucene.util.automaton;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestAutomatonStructuralComparator extends LuceneTestCase {

  public void testStructuralAutomatonEqualsAndHashCodeForEqualAutomata() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeDeterministicTwoBranchAutomaton('a', 'b');
    assertTrue(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
    assertEquals(
        AutomatonStructuralComparator.structuralAutomatonHashCode(left),
        AutomatonStructuralComparator.structuralAutomatonHashCode(right));
  }

  public void testStructuralAutomatonEqualsForDifferentTransitions() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeDeterministicTwoBranchAutomaton('a', 'c');
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
  }

  public void testStructuralAutomatonEqualsForDifferentAcceptStates() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeDeterministicTwoBranchAutomaton('a', 'b');
    right.setAccept(1, false);
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
  }

  public void testStructuralAutomatonEqualsWithSameReference() {
    Automaton automaton = makeDeterministicTwoBranchAutomaton('x', 'y');
    assertTrue(AutomatonStructuralComparator.structuralAutomatonEquals(automaton, automaton));
  }

  public void testStructuralAutomatonEqualsForDifferentNumberOfStates() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeSingleTransitionAutomaton('a');
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
  }

  public void testStructuralAutomatonEqualsForDifferentNumberOfTransitions() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeSingleTransitionAutomaton('a');
    right.createState();
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
  }

  public void testStructuralAutomatonEqualsForDifferentTransitionDestination() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeDeterministicTwoBranchAutomatonWithSecondDest('a', 'b', 1);
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
  }

  public void testStructuralAutomatonEqualsAndHashCodeForDifferentTransitionRange() {
    Automaton left = makeDeterministicTwoBranchAutomaton('a', 'b');
    Automaton right = makeDeterministicTwoBranchAutomaton('a', 'c');
    assertFalse(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
    assertNotEquals(
        AutomatonStructuralComparator.structuralAutomatonHashCode(left),
        AutomatonStructuralComparator.structuralAutomatonHashCode(right));
  }

  public void testStructuralAutomatonEqualsIsSymmetric() {
    Automaton left = makeDeterministicTwoBranchAutomaton('m', 'n');
    Automaton right = makeDeterministicTwoBranchAutomaton('m', 'n');
    assertTrue(AutomatonStructuralComparator.structuralAutomatonEquals(left, right));
    assertTrue(AutomatonStructuralComparator.structuralAutomatonEquals(right, left));
  }

  private static Automaton makeDeterministicTwoBranchAutomaton(char first, char second) {
    Automaton automaton = new Automaton();
    int start = automaton.createState();
    int accept1 = automaton.createState();
    int accept2 = automaton.createState();
    automaton.setAccept(accept1, true);
    automaton.setAccept(accept2, true);
    automaton.addTransition(start, accept1, first, first);
    automaton.addTransition(start, accept2, second, second);
    automaton.finishState();
    return automaton;
  }

  private static Automaton makeSingleTransitionAutomaton(char label) {
    Automaton automaton = new Automaton();
    int start = automaton.createState();
    int accept = automaton.createState();
    automaton.setAccept(accept, true);
    automaton.addTransition(start, accept, label, label);
    automaton.finishState();
    return automaton;
  }

  private static Automaton makeDeterministicTwoBranchAutomatonWithSecondDest(
      char first, char second, int secondDest) {
    Automaton automaton = new Automaton();
    int start = automaton.createState();
    int accept1 = automaton.createState();
    int accept2 = automaton.createState();
    automaton.setAccept(accept1, true);
    automaton.setAccept(accept2, true);
    automaton.addTransition(start, accept1, first, first);
    automaton.addTransition(start, secondDest, second, second);
    automaton.finishState();
    return automaton;
  }
}
