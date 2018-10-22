package com.streamingdata.collection.service;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class CollectionServiceWebSocketClient {
    private static boolean continueRunning = true;

    public static void main(String[] args) throws Exception {
        HybridMessageLogger.initialize();
        final String URL = 0 < args.length ? args[0] : "ws://stream.meetup.com/2/rsvps";
        URI uri = new URI(URL);
        final int port = 80;
        WebSocketClientHandshaker webSocketClientHandshaker = WebSocketClientHandshakerFactory.newHandshaker(uri,
                WebSocketVersion.V13, null, false, new DefaultHttpHeaders());
        final RSVPProducer rsvpProducer = new RSVPProducer();
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            shutdownHook(rsvpProducer, group);

            MeetUpWebSocketClientHandler handler = new MeetUpWebSocketClientHandler(webSocketClientHandshaker, rsvpProducer);

            System.out.println("Prepare bootstrap start");
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    ChannelPipeline p = socketChannel.pipeline();
                    p.addLast(new HttpClientCodec(),
                            new HttpObjectAggregator(8192),
                            WebSocketClientCompressionHandler.INSTANCE,
                            handler);
                }
            });
            System.out.println("Prepare bootstrap end");
            Channel channel = b.connect(uri.getHost(), port).sync().channel();
            System.out.println("Channel connected");
            handler.handshakeFuture().sync();
            System.out.println("Handler hadnshaked ");
            BufferedReader console = new BufferedReader(new InputStreamReader(System.in));

            do {
                String msg = console.readLine();
                if (msg == null) {
                    continueRunning = false;
                } else if ("bye".equals(msg.toLowerCase())) {
                    channel.writeAndFlush(new CloseWebSocketFrame());
                    channel.closeFuture().sync();
                    continueRunning = false;
                } else if ("ping".equals(msg.toLowerCase())) {
                    WebSocketFrame frame = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] { 8, 1, 8, 1 }));
                    channel.writeAndFlush(frame);
                } else {
                    WebSocketFrame frame = new TextWebSocketFrame(msg);
                    channel.writeAndFlush(frame);
                }
            } while (continueRunning);
        } finally {
            group.shutdownGracefully();
        }


    }

    private static void shutdownHook(final RSVPProducer rsvpProducer, final EventLoopGroup group) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Shutdown started...");
                    group.shutdownGracefully();
                    rsvpProducer.close();
                    HybridMessageLogger.close();
                    continueRunning = false;
                    System.out.println("Shutdown finished");
                } catch (final Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }
}
