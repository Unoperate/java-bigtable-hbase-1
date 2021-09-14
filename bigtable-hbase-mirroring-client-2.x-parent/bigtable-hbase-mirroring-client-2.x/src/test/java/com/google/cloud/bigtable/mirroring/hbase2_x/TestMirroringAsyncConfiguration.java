/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringAsyncConfiguration {
  private Exception assertInvalidConfiguration(final Configuration test) {
    return assertThrows(
        IllegalArgumentException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            new MirroringAsyncConfiguration(test);
          }
        });
  }

  @Test
  public void testRequiresConfiguringImplClasses() {
    Configuration testConfiguration = new Configuration(false);
    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.connection.impl");

    testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.async.connection.impl");

    testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.secondary-client.async.connection.impl");
  }

  @Test
  public void testSameConnectionClassesRequireOneOfPrefixes() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "2");
    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc).hasMessageThat().contains("prefix");

    testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "0");
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc).hasMessageThat().contains("prefix");

    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "3");
    new MirroringAsyncConfiguration(testConfiguration);
  }

  @Test
  public void testDifferentConnectionClassesDoNotRequirePrefix() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "4");
    new MirroringAsyncConfiguration(testConfiguration);
  }

  @Test
  public void testSamePrefixForPrimaryAndSecondaryIsNotAllowedIfConnectionClassesAreTheSame() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "test");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "test");

    assertInvalidConfiguration(testConfiguration);

    testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "0");
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "test");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "test");

    assertInvalidConfiguration(testConfiguration);
  }

  @Test
  public void testConfigWithoutPrefixReceivesAllProperties() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    testConfiguration.set("connection-1.test1", "11");
    testConfiguration.set("connection-1.test2", "12");
    testConfiguration.set("not-a-connection-1.test3", "13");

    testConfiguration.set("test1", "1");
    testConfiguration.set("test2", "2");

    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("test1")).isEqualTo("11");
    assertThat(configuration.primaryConfiguration.get("test2")).isEqualTo("12");
    // expected test1, test2, hbase.client.connection.impl, hbase.client.async.connection.impl
    assertThat(configuration.primaryConfiguration.getValByRegex(".*")).hasSize(4);

    assertThat(configuration.secondaryConfiguration.get("test1")).isEqualTo("1");
    assertThat(configuration.secondaryConfiguration.get("test2")).isEqualTo("2");
    assertThat(configuration.secondaryConfiguration.get("connection-1.test1")).isEqualTo("11");
    assertThat(configuration.secondaryConfiguration.get("connection-1.test2")).isEqualTo("12");
  }

  @Test
  public void testConfigurationPrefixesAreStrippedAndPassedAsConfigurations() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "connection-2");
    testConfiguration.set("connection-2.test1", "21");
    testConfiguration.set("connection-2.test2", "22");

    testConfiguration.set("connection-1.test1", "11");
    testConfiguration.set("connection-1.test2", "12");

    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("test1")).isEqualTo("11");
    assertThat(configuration.primaryConfiguration.get("test2")).isEqualTo("12");
    assertThat(configuration.secondaryConfiguration.get("test1")).isEqualTo("21");
    assertThat(configuration.secondaryConfiguration.get("test2")).isEqualTo("22");
  }

  @Test
  public void testMirroringOptionsAreRead() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "test");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "test");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS, "test-1");

    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);

    assertThat(configuration.mirroringOptions.flowControllerStrategyClass).isEqualTo("test-1");
  }

  @Test
  public void TestMirroringAsyncConnectionClassesArePassedAsHbaseConfig() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "test1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "test2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        TestMirroringAsyncConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("test1");
    assertThat(configuration.secondaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("test2");
  }

  @Test
  public void testConstructors() {
    Configuration emptyConfiguration = new Configuration(false);
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringAsyncConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    testConfiguration.set(
        MirroringAsyncConfiguration.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "4");

    MirroringConfiguration mirroringConfiguration =
        new MirroringConfiguration(emptyConfiguration, emptyConfiguration, emptyConfiguration);
    MirroringAsyncConfiguration mirroringAsyncConfiguration =
        new MirroringAsyncConfiguration(emptyConfiguration, emptyConfiguration, emptyConfiguration);

    MirroringAsyncConfiguration mirroringAsyncConfiguration3 =
        new MirroringAsyncConfiguration(testConfiguration);
    MirroringConfiguration mirroringConfiguration3 = new MirroringConfiguration(testConfiguration);

    new MirroringConfiguration(testConfiguration);
    new MirroringConfiguration(mirroringConfiguration);
    new MirroringConfiguration(mirroringConfiguration3);
    new MirroringConfiguration(mirroringAsyncConfiguration);
    new MirroringConfiguration(mirroringAsyncConfiguration3);

    new MirroringAsyncConfiguration(testConfiguration);
    new MirroringAsyncConfiguration(mirroringConfiguration);
    new MirroringAsyncConfiguration(mirroringConfiguration3);
    new MirroringAsyncConfiguration(mirroringAsyncConfiguration);
    new MirroringAsyncConfiguration(mirroringAsyncConfiguration3);
  }
}
