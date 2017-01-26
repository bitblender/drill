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
package org.apache.drill.exec.rpc.control;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;
import java.util.UUID;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.AbstractServerConnection;
import org.apache.drill.exec.rpc.ClientConnection;
import org.apache.drill.exec.rpc.RequestHandler;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.protobuf.MessageLite;
import org.apache.drill.exec.rpc.security.AuthenticatorProvider;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

public class ControlConnection extends AbstractServerConnection<ControlConnection> implements ClientConnection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlConnection.class);

  private final RpcBus<RpcType, ControlConnection> bus;
  private final BufferAllocator allocator;
  private volatile DrillbitEndpoint endpoint;
  private volatile boolean active = false;
  private final UUID id;

  private SaslClient saslClient;

  public ControlConnection(String name, SocketChannel channel, RpcBus<RpcType, ControlConnection> bus,
                           BufferAllocator allocator, AuthenticatorProvider authProvider,
                           RequestHandler<ControlConnection> handler) {
    super(channel, name, authProvider, handler);
    this.bus = bus;
    this.id = UUID.randomUUID();
    this.allocator = allocator;
  }

  void setEndpoint(DrillbitEndpoint endpoint) {
    assert this.endpoint == null : "Endpoint should only be set once (only in the case in incoming server requests).";
    this.endpoint = endpoint;
    active = true;
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> outcomeListener,
      RpcType rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    bus.send(outcomeListener, this, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void sendUnsafe(RpcOutcomeListener<RECEIVE> outcomeListener,
      RpcType rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    bus.send(outcomeListener, this, rpcType, protobufBody, clazz, true, dataBodies);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ControlConnection other = (ControlConnection) obj;
    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public void setSaslClient(final SaslClient saslClient) {
    assert this.saslClient == null;
    this.saslClient = saslClient;
  }

  @Override
  public SaslClient getSaslClient() {
    assert saslClient != null;
    return saslClient;
  }

  @Override
  public void finalizeSession() throws IOException {
     // TODO: MUST FIX
  }

  @Override
  public void close() {
    try {
      if (saslClient != null) {
        saslClient.dispose();
        saslClient = null;
      }
    } catch (final SaslException e) {
      logger.warn("Unclean disposal", e);
    }
    super.close();
  }

}
