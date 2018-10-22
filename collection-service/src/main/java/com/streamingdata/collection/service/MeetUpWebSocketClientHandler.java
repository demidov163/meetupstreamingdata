package com.streamingdata.collection.service;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.util.Random;
import java.util.UUID;

public class MeetUpWebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private final RSVPProducer rsvpProducer;
    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private Random random;


    public MeetUpWebSocketClientHandler(WebSocketClientHandshaker handshaker, RSVPProducer rsvpProducer) {
        this.handshaker = handshaker;
        this.rsvpProducer = rsvpProducer;
        random = new Random();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("WebSocket Client disconnected!");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("channelRead0 start ");
        Channel channel = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(channel, (FullHttpResponse) msg);
            handshakeFuture.setSuccess();
            System.out.println("channelRead0 Handshake is no set ");
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(String.format("Unexpected FullHttpResponse (getStatus=%s, content= %s)",
                    response.getStatus().toString(), response.content().toString()));
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            System.out.println("channelRead0 TextWebSocketFrame " + textFrame.content());

            String messageKey = new UUID(random.nextLong(), random.nextLong()).toString();
            byte[] messagePayload = new byte[textFrame.content().readableBytes()];
            textFrame.content().readBytes(messagePayload);
            HybridMessageLogger.addEvent(messageKey, messagePayload);
            rsvpProducer.sendMessage(messageKey, messagePayload);
        } else if (frame instanceof CloseWebSocketFrame) {
            //server ask to close the connection
            System.out.println("channelRead0 close ");
            channel.close();
            rsvpProducer.close();
        }
    }

    public ChannelPromise handshakeFuture() {
        return handshakeFuture;
    }
}
