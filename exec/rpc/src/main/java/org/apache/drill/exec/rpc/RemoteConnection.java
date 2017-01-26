/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc;

import io.netty.channel.Channel;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;

import java.net.SocketAddress;

public interface RemoteConnection extends ConnectionThrottle, AutoCloseable {

  boolean inEventLoop();

  String getName();

  BufferAllocator getAllocator();

  Channel getChannel();

  boolean blockOnNotWritable(RpcOutcomeListener<?> listener);

  boolean isActive();

  <V> RpcOutcome<V> getAndRemoveRpcOutcome(int rpcType, int coordinationId, Class<V> clazz);

  <V> ChannelListenerWithCoordinationId createNewRpcListener(RpcOutcomeListener<V> handler, Class<V> clazz);

  void recordRemoteFailure(int coordinationId, UserBitShared.DrillPBError failure);

  void channelClosed(RpcException ex);

  SocketAddress getRemoteAddress();

  @Override
  void close();

}
