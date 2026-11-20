/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.socket.nio;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.util.internal.SocketUtils;
import io.netty.channel.nio.AbstractNioMessageChannel;
import io.netty.channel.socket.DefaultServerSocketChannelConfig;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SuppressJava6Requirement;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.Map;

/**
 * A {@link io.netty.channel.socket.ServerSocketChannel} implementation which uses
 * NIO selector based implementation to accept new connections.
 */
/*
 * 该类是Netty中对服务端非阻塞网络通道的最终实现。
 * 由于大部分操作都在父类中实现，所以该类中只定义了负责接收客户端连接并创建通道对象的方法。
 */
public class NioServerSocketChannel extends AbstractNioMessageChannel
                             implements io.netty.channel.socket.ServerSocketChannel {

    // SNIPPET_START fields
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    /*
     * 这里获取JDK层面的SelectorProvider，在Linux系统中是EPollSelectorProvider
     */
    private static final SelectorProvider DEFAULT_SELECTOR_PROVIDER = SelectorProvider.provider();

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioServerSocketChannel.class);

    private static final Method OPEN_SERVER_SOCKET_CHANNEL_WITH_FAMILY =
            SelectorProviderUtil.findOpenMethod("openServerSocketChannel");
    // SNIPPET_END fields

    // SNIPPET_START newChannel
    /*
     * 创建服务端网络通道
     */
    private static ServerSocketChannel newChannel(SelectorProvider provider, InternetProtocolFamily family) {
        try {
            // 创建通道
            ServerSocketChannel channel =
                    SelectorProviderUtil.newChannel(OPEN_SERVER_SOCKET_CHANNEL_WITH_FAMILY, provider, family);
            /*
             * 这里通过JDK的SelectorProvider来创建服务端网络通道，具体逻辑参考SelectorProviderImpl。
             */
            return channel == null ? provider.openServerSocketChannel() : channel;
        } catch (IOException e) {
            throw new ChannelException("Failed to open a socket.", e);
        }
    }
    // SNIPPET_END newChannel

    private final ServerSocketChannelConfig config;

    // SNIPPET_START constructors
    /**
     * Create a new instance
     */
    public NioServerSocketChannel() {
        // 传入默认的选择器提供者
        this(DEFAULT_SELECTOR_PROVIDER);
    }

    /**
     * Create a new instance using the given {@link SelectorProvider}.
     */
    public NioServerSocketChannel(SelectorProvider provider) {
        this(provider, null);
    }

    /**
     * Create a new instance using the given {@link SelectorProvider} and protocol family (supported only since JDK 15).
     */
    public NioServerSocketChannel(SelectorProvider provider, InternetProtocolFamily family) {
        // 先创建JDK层面的ServerSocketChannel，再对其进行封装。
        this(newChannel(provider, family));
    }

    /**
     * Create a new instance using the given {@link ServerSocketChannel}.
     */
    public NioServerSocketChannel(ServerSocketChannel channel) {
        /*
         * 通过父类构造器来进行封装，这里传入了该channel所感兴趣的事件为OP_ACCEPT，表示处理客户端的连接。
         */
        super(null, channel, SelectionKey.OP_ACCEPT);
        /*
         * 创建配置
         * 这里传入了JDK层面的ServerSocketChannel中的ServerSocket对象
         */
        config = new NioServerSocketChannelConfig(this, javaChannel().socket());
    }
    // SNIPPET_END constructors

    @Override
    public InetSocketAddress localAddress() {
        return (InetSocketAddress) super.localAddress();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    @Override
    public ServerSocketChannelConfig config() {
        return config;
    }

    @Override
    public boolean isActive() {
        // As java.nio.ServerSocketChannel.isBound() will continue to return true even after the channel was closed
        // we will also need to check if it is open.
        /*
         * 这里会判断底层JDK的Channel是否没有被关闭，以及是否已经绑定。
         */
        return isOpen() && javaChannel().socket().isBound();
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return null;
    }

    @Override
    protected ServerSocketChannel javaChannel() {
        return (ServerSocketChannel) super.javaChannel();
    }

    @Override
    protected SocketAddress localAddress0() {
        return SocketUtils.localSocketAddress(javaChannel().socket());
    }

    // SNIPPET_START doBind
    @SuppressJava6Requirement(reason = "Usage guarded by java version check")
    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (PlatformDependent.javaVersion() >= 7) { // JDK版本大于等于7的情况
            // 通过底层JDK的通道执行bind操作，注意内部不仅会bind，还会listen
            javaChannel().bind(localAddress, config.getBacklog());
        } else {
            javaChannel().socket().bind(localAddress, config.getBacklog());
        }
    }
    // SNIPPET_END doBind

    @Override
    protected void doClose() throws Exception {
        javaChannel().close();
    }

    // SNIPPET_START doReadMessages
    @Override
    protected int doReadMessages(List<Object> buf) throws Exception {
        // 执行Accept，创建JDK层面的channel，用于表示新连接
        SocketChannel ch = SocketUtils.accept(javaChannel());

        try {
            if (ch != null) {
                // 将新连接通道封装为NioSocketChannel对象
                buf.add(new NioSocketChannel(this, ch));
                return 1;
            }
        } catch (Throwable t) {
            logger.warn("Failed to create a new channel from an accepted socket.", t);

            try {
                ch.close();
            } catch (Throwable t2) {
                logger.warn("Failed to close a socket.", t2);
            }
        }

        return 0;
    }
    // SNIPPET_END doReadMessages

    // Unnecessary stuff
    @Override
    protected boolean doConnect(
            SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doFinishConnect() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return null;
    }

    @Override
    protected void doDisconnect() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    private final class NioServerSocketChannelConfig extends DefaultServerSocketChannelConfig {
        private NioServerSocketChannelConfig(NioServerSocketChannel channel, ServerSocket javaSocket) {
            super(channel, javaSocket);
        }

        @Override
        protected void autoReadCleared() {
            clearReadPending();
        }

        @Override
        public <T> boolean setOption(ChannelOption<T> option, T value) {
            if (PlatformDependent.javaVersion() >= 7 && option instanceof NioChannelOption) {
                return NioChannelOption.setOption(jdkChannel(), (NioChannelOption<T>) option, value);
            }
            return super.setOption(option, value);
        }

        @Override
        public <T> T getOption(ChannelOption<T> option) {
            if (PlatformDependent.javaVersion() >= 7 && option instanceof NioChannelOption) {
                return NioChannelOption.getOption(jdkChannel(), (NioChannelOption<T>) option);
            }
            return super.getOption(option);
        }

        @Override
        public Map<ChannelOption<?>, Object> getOptions() {
            if (PlatformDependent.javaVersion() >= 7) {
                return getOptions(super.getOptions(), NioChannelOption.getOptions(jdkChannel()));
            }
            return super.getOptions();
        }

        private ServerSocketChannel jdkChannel() {
            return ((NioServerSocketChannel) channel).javaChannel();
        }
    }

    // Override just to to be able to call directly via unit tests.
    @Override
    protected boolean closeOnReadError(Throwable cause) {
        return super.closeOnReadError(cause);
    }
}
