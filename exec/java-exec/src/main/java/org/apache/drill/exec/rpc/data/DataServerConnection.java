/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.data;

import io.netty.channel.socket.SocketChannel;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.rpc.security.ServerAuthenticationHandler;
import org.apache.drill.exec.rpc.AbstractServerConnection;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import javax.security.sasl.SaslException;
import java.io.IOException;

// data connection on server-side (i.e. bit handling request or receiving data)
public class DataServerConnection extends AbstractServerConnection<DataServerConnection> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataServerConnection.class);

  DataServerConnection(SocketChannel channel, ServerConnectionConfigImpl config) {
    super(channel, config, config.getAuthProvider() == null
        ? config.getMessageHandler()
        : new ServerAuthenticationHandler<>(config.getMessageHandler(),
        RpcType.SASL_MESSAGE_VALUE, RpcType.SASL_MESSAGE));
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public void finalizeSession() throws IOException {
    final String authorizationID = getSaslServer().getAuthorizationID();
    final String remoteShortName = new HadoopKerberosName(authorizationID).getShortName();
    final String localShortName = UserGroupInformation.getLoginUser().getShortUserName();
    if (!localShortName.equalsIgnoreCase(remoteShortName)) {
      throw new SaslException(String.format("'primary' of remote drillbit's service principal " +
          "does not match with this drillbit's. Expected: %s Actual: %s", localShortName, remoteShortName));
    }
    logger.debug("Authenticated data connection for {}", authorizationID);
  }
}
