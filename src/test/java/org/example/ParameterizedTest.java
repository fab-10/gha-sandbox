package org.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.provider.ValueSource;

public class ParameterizedTest {

  @org.junit.jupiter.params.ParameterizedTest
  @ValueSource(booleans = {true})
  public void test(final boolean value) {
    Assertions.assertTrue(value);
  }
}
