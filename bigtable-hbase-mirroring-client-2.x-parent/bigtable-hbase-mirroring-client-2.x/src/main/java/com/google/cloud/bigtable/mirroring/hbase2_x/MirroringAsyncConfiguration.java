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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import org.apache.hadoop.conf.Configuration;

public class MirroringAsyncConfiguration extends MirroringConfiguration {
  public MirroringAsyncConfiguration(
      Configuration primaryConfiguration,
      Configuration secondaryConfiguration,
      Configuration mirroringConfiguration) {
    super(primaryConfiguration, secondaryConfiguration, mirroringConfiguration);
    super.set(
        "hbase.client.async.connection.impl", MirroringAsyncConnection.class.getCanonicalName());
  }

  public MirroringAsyncConfiguration(Configuration conf) {
    super(conf);
  }

  @Override
  protected void fillConnectionConfigWithClassImplementations(
      Configuration connectionConfig,
      Configuration conf,
      String connectionClassKey,
      String asyncConnectionClassKey) {
    super.fillConnectionConfigWithClassImplementations(
        connectionConfig, conf, connectionClassKey, asyncConnectionClassKey);
    String asyncConnectionClassName = conf.get(asyncConnectionClassKey);
    if (!asyncConnectionClassKey.equalsIgnoreCase("default")) {
      connectionConfig.set("hbase.client.async.connection.impl", asyncConnectionClassName);
    }
  }

  @Override
  protected void verifyParameters(Configuration conf) {
    super.verifyParameters(conf);
    checkParameters(
        conf,
        MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY);
  }
}
