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
package org.apache.drill.exec.rpc;

import io.netty.channel.socket.SocketChannel;
import org.apache.drill.exec.rpc.security.AuthenticatorProvider;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;

public abstract class AbstractServerConnection<C extends AbstractServerConnection>
    extends AbstractRemoteConnection
    implements ServerConnection<C> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractServerConnection.class);

  private final AuthenticatorProvider authProvider;

  private RequestHandler<C> currentHandler;
  private SaslServer saslServer;

  public AbstractServerConnection(SocketChannel channel, String name, AuthenticatorProvider authProvider,
                                  RequestHandler<C> handler) {
    super(channel, name);
    assert handler != null;
    this.authProvider = authProvider;
    currentHandler = handler;
  }

  @Override
  public void initSaslServer(String mechanismName) throws IllegalArgumentException, SaslException {
    assert saslServer == null && authProvider != null;
    try {
      this.saslServer = authProvider.getAuthenticatorFactory(mechanismName)
          .createSaslServer(UserGroupInformation.getLoginUser(), null
              /** properties; default QOP is auth */);
    } catch (final IOException e) {
      logger.debug("Login failed.", e);
      final Throwable cause = e.getCause();
      if (cause instanceof LoginException) {
        throw new SaslException("Failed to login.", cause);
      }
      throw new SaslException("Unexpected failure trying to login.", cause);
    }
    if (saslServer == null) {
      throw new SaslException("Server could not initiate authentication. Insufficient parameters?");
    }
  }

  @Override
  public SaslServer getSaslServer() {
    assert saslServer != null;
    return saslServer;
  }

  @Override
  public RequestHandler<C> getCurrentHandler() {
    return currentHandler;
  }

  @Override
  public void changeHandlerTo(final RequestHandler<C> handler) {
    assert handler != null;
    this.currentHandler = handler;
  }

  @Override
  public void close() {
    try {
      if (saslServer != null) {
        saslServer.dispose();
        saslServer = null;
      }
    } catch (final SaslException e) {
      logger.warn("Unclean disposal.", e);
    }
    super.close();
  }
}
