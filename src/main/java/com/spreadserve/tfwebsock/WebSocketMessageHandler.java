package com.spreadserve.tfwebsock;

import io.netty.channel.ChannelHandlerContext;

/**
 * Created by jwb on 3/13/15.
 */
public interface WebSocketMessageHandler {
    public void handleMessage(ChannelHandlerContext ctx, String frameText);
    public void disconnected( ChannelHandlerContext ctx);
}
