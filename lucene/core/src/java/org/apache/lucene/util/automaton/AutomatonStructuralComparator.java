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

/**
 * Utility methods for comparing {@link Automaton} instances by concrete graph structure.
 *
 * <p>The comparisons in this class are <b>structural</b>: they operate on state ordinals,
 * accept-state flags, and transition tuples as stored in each automaton. They do not attempt
 * language equivalence (for example, via minimization or state renaming).
 *
 * <p>As a result, two automata that accept the same language may compare as different if their
 * internal graph representation differs.
 */
final class AutomatonStructuralComparator {

  private AutomatonStructuralComparator() {}

  /**
   * Computes a hash code from the automaton's concrete graph layout (states, accept flags,
   * transition counts, and transition tuples in iteration order).
   *
   * <p>This is a <b>structural</b> hash, not a language-equivalence hash: two automata that accept
   * the same language but have different graph shapes or state numbering may produce different hash
   * codes.
   *
   * <p>Intended for pairing with {@link #structuralAutomatonEquals(Automaton, Automaton)}.
   */
  static int structuralAutomatonHashCode(Automaton automaton) {
    int result = automaton.getNumStates();
    Transition transition = new Transition();
    for (int state = 0; state < automaton.getNumStates(); state++) {
      result = 31 * result + (automaton.isAccept(state) ? 1 : 0);
      int numTransitions = automaton.initTransition(state, transition);
      result = 31 * result + numTransitions;
      for (int i = 0; i < numTransitions; i++) {
        automaton.getNextTransition(transition);
        result = 31 * result + transition.dest;
        result = 31 * result + transition.min;
        result = 31 * result + transition.max;
      }
    }
    return result;
  }

  /**
   * Returns {@code true} if both automata have the same concrete graph structure: same number of
   * states, same accept/non-accept flags per state ordinal, same number of outgoing transitions per
   * state, and identical transition triples ({@code dest}, {@code min}, {@code max}) in iteration
   * order.
   *
   * <p>This is a <b>structural</b> comparison, not language equivalence. Two automata can accept
   * exactly the same language and still return {@code false} if their internal representation
   * differs.
   */
  static boolean structuralAutomatonEquals(Automaton left, Automaton right) {
    if (left == right) {
      return true;
    }
    if (left.getNumStates() != right.getNumStates()) {
      return false;
    }
    Transition leftTransition = new Transition();
    Transition rightTransition = new Transition();
    for (int state = 0; state < left.getNumStates(); state++) {
      if (left.isAccept(state) != right.isAccept(state)) {
        return false;
      }
      int leftNumTransitions = left.initTransition(state, leftTransition);
      int rightNumTransitions = right.initTransition(state, rightTransition);
      if (leftNumTransitions != rightNumTransitions) {
        return false;
      }
      for (int i = 0; i < leftNumTransitions; i++) {
        left.getNextTransition(leftTransition);
        right.getNextTransition(rightTransition);
        if (leftTransition.dest != rightTransition.dest
            || leftTransition.min != rightTransition.min
            || leftTransition.max != rightTransition.max) {
          return false;
        }
      }
    }
    return true;
  }
}
