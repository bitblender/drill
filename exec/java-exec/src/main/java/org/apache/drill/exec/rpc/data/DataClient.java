/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.data;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcCommand;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.security.AuthenticationOutcomeListener;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.security.AuthenticatorProvider;
import org.apache.drill.exec.server.BootStrapContext;

import com.google.protobuf.MessageLite;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;

public class DataClient extends BasicClient<RpcType, DataClientConnection, BitClientHandshake, BitServerHandshake>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataClient.class);

  private final DrillbitEndpoint remoteEndpoint;
  private volatile DataClientConnection connection;
  private final BufferAllocator allocator;
  private final DataConnectionManager.CloseHandlerCreator closeHandlerFactory;
  private final String authMechanismToUse;
  private final AuthenticatorProvider authProvider;


  public DataClient(DrillbitEndpoint remoteEndpoint, BootStrapContext context,
                    DataConnectionManager.CloseHandlerCreator closeHandlerFactory) {
    super(
        DataRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        context.getAllocator().getAsByteBufAllocator(),
        context.getBitClientLoopGroup(),
        RpcType.HANDSHAKE,
        BitServerHandshake.class,
        BitServerHandshake.PARSER);
    this.remoteEndpoint = remoteEndpoint;
    this.closeHandlerFactory = closeHandlerFactory;
    this.allocator = context.getAllocator();
    if (context.getConfig().getBoolean(ExecConstants.BIT_AUTHENTICATION_ENABLED)) {
      this.authProvider = context.getAuthProvider();
      this.authMechanismToUse = context.getConfig()
          .getString(ExecConstants.BIT_AUTHENTICATION_MECHANISM)
          .toLowerCase();
    } else {
      this.authProvider = null;
      this.authMechanismToUse = null;
    }
  }

  @Override
  public DataClientConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    this.connection = new DataClientConnection(channel, this);
    return connection;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch, DataClientConnection clientConnection) {
    return closeHandlerFactory.getHandler(clientConnection, super.getCloseHandler(ch, clientConnection));
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DataDefaultInstanceHandler.getResponseDefaultInstanceClient(rpcType);
  }

  @Override
  protected void handle(DataClientConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody,
                        ResponseSender sender) throws RpcException {
    throw new UnsupportedOperationException("DataClient is unidirectional by design.");
  }

  BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  protected void validateHandshake(BitServerHandshake handshake) throws RpcException {
    if (handshake.getRpcVersion() != DataRpcConfig.RPC_VERSION) {
      throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.",
          handshake.getRpcVersion(), DataRpcConfig.RPC_VERSION));
    }

    if (handshake.getAuthenticationMechanismsCount() != 0) { // remote requires authentication
      if (authProvider == null) {
        throw new RpcException(
            String.format("Drillbit running on %s requires authentication, but authentication is not configured.",
                remoteEndpoint.getAddress()));
      }

      if (!handshake.getAuthenticationMechanismsList().contains(authMechanismToUse)) {
        throw new RpcException(String.format("Drillbit running on %s does not support %s",
            remoteEndpoint.getAddress(), authMechanismToUse));
      }
      final SaslClient saslClient;
      try {
        saslClient = authProvider.getAuthenticatorFactory(authMechanismToUse)
            .createSaslClient(UserGroupInformation.getLoginUser(), null /** properties; default QOP is auth */); // TODO FIX
      } catch (final IOException e) {
        throw new RpcException("Unexpected failure trying to login.", e);
      }
      if (saslClient == null) {
        throw new RpcException("Unexpected failure. Could not initiate authentication.");
      }
      connection.setSaslClient(saslClient);
    }
  }

  @Override
  protected void finalizeConnection(BitServerHandshake handshake, DataClientConnection connection) {
  }

  @Override
  protected <M extends MessageLite> RpcCommand<M, DataClientConnection>
  getInitialCommand(final RpcCommand<M, DataClientConnection> command) {
    if (authProvider == null) {
      return super.getInitialCommand(command);
    } else {
      return new AuthenticationCommand<>(command);
    }
  }

  private class AuthenticationCommand<M extends MessageLite> implements RpcCommand<M, DataClientConnection> {

    private final RpcCommand<M, DataClientConnection> command;

    public AuthenticationCommand(RpcCommand<M, DataClientConnection> command) {
      this.command = command;
    }

    @Override
    public void connectionAvailable(DataClientConnection connection) {
      command.connectionFailed(FailureType.AUTHENTICATION, new SaslException("Should not reach here."));
    }

    @Override
    public void connectionSucceeded(final DataClientConnection connection) {
      try {
        new AuthenticationOutcomeListener<>(DataClient.this, connection, RpcType.SASL_MESSAGE,
            UserGroupInformation.getLoginUser(),
            new RpcOutcomeListener<Void>() {
              @Override
              public void failed(RpcException ex) {
                command.connectionFailed(FailureType.AUTHENTICATION, ex);
              }

              @Override
              public void success(Void value, ByteBuf buffer) {
                command.connectionSucceeded(connection);
              }

              @Override
              public void interrupted(InterruptedException e) {
                command.connectionFailed(FailureType.AUTHENTICATION, e);
              }
            }).initiate(authMechanismToUse);
      } catch (IOException e) {
        command.connectionFailed(FailureType.AUTHENTICATION, e);
      }
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      command.connectionFailed(FailureType.AUTHENTICATION, t);
    }
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new DataProtobufLengthDecoder.Client(allocator, OutOfMemoryHandler.DEFAULT_INSTANCE);
  }
}
