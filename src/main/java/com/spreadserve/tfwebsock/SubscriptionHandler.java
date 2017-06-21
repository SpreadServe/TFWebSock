package com.spreadserve.tfwebsock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class SubscriptionHandler implements WebSocketMessageHandler {


    private static Logger logr = LoggerFactory.getLogger( SubscriptionHandler.class);

    // stateless JSON serializer/deserializer
    private Gson gson = new Gson();
    private LinkedBlockingQueue<JSON.Message>   outQ;
    private Map<String,Map<String,Map<String,Channel>>>  subscriptions = new HashMap<String,Map<String,Map<String,Channel>>>( );


    public SubscriptionHandler(LinkedBlockingQueue<JSON.Message> outq)
    {
        this.outQ = outq;
    }

    public void handleMessage(ChannelHandlerContext ctx, String frameText) {
        SubscriptionRequest tickerRequest = gson.fromJson(frameText, SubscriptionRequest.class);
        String action = tickerRequest.getAction( );
        if ( action.equals( "subscribe")) {
            Subscribe(tickerRequest, ctx.channel( ));
        }
    }

    public void disconnected(ChannelHandlerContext ctx)
    {
        JSON.ClientDisconnectMessage dmsg = new JSON.ClientDisconnectMessage( ctx.channel( ));
        try {
            outQ.put( dmsg);
        }
        catch (InterruptedException e) {
            logr.info( "StockTickerMessageHandler.disconnected: outQ.put( ) failed for "
                                                                + ctx + "\n" + e.toString( ));
        }
    }

    public void Subscribe( SubscriptionRequest req, Channel chan)
    {
        // Top level: get the sub map that hold the subscriptions for the relevant ecn
        Map<String,Map<String,Channel>> ecnmap = null;
        String ecn = req.getECN( );
        if ( subscriptions.containsKey( ecn)) {
            ecnmap = subscriptions.get( ecn);
        }
        else {
            ecnmap = new HashMap<String,Map<String,Channel>>( );
            subscriptions.put( ecn, ecnmap);
        }
        // Now let's find if there are existing subs for the instrument in question on this ecn
        Map<String,Channel> submap = null;
        String instrument = req.getInstrument( );
        if ( ecnmap.containsKey( instrument)) {
            submap = ecnmap.get( instrument);
        }
        else {
            submap = new HashMap<String,Channel>( );
            ecnmap.put(instrument, submap);
        }
        // Is there already an entry for this channel?
        if ( submap.containsValue( chan)) {
            logr.info( "StockTickerMessageHandler.Subscribe: " + ecn + " already has sub for " + instrument + " on " + chan.toString( ));
            return;
        }
        JSON.MarketDataSubscriptionRequestMessage sreq = new JSON.MarketDataSubscriptionRequestMessage( ecn, instrument, chan);
        try {
            outQ.put( sreq);
        }
        catch (InterruptedException e) {
            logr.info( "StockTickerMessageHandler.Subscribe: outQ.put( ) failed for "
                       + ecn + ":" + instrument + "\n" + e.toString( ));
        }
    }
}
