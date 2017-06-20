package com.spreadserve.tfwebsock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.transficc.sample.outbound.TransficcService;
import com.transficc.sample.util.InstructionIdGenerator;


import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;


class WebSockPusher implements Callable<String>
{
    private static Logger logr = Logger.getLogger("com.transficc.sample.xl.WebSockPusher");
    private static WebSockPusher instance = null;

    private static final AtomicBoolean keepRunning = new AtomicBoolean(true);
    // After logon to a venue we get a callback giving us the venue sessionId with the instructionId. This maps instructionId
    // to ECN name. So when we later get market data updates we can map from sessionId to ECN name.
    // instructionId:ecn eg "iswap ins id":"Iswap"
    private Map<String,String>  venueInstructionIdMap = new HashMap<String,String>( );
    // one per callable as it is stateless, but not thread safe
    private Gson gson = new Gson();
    private LinkedBlockingQueue<JSON.Message> outQ;
    private final InstructionIdGenerator instructionIdGenerator = new InstructionIdGenerator( );
    // instructionId:[{ecn,instrument,channel},...]
    // used when an update arrives to determine which clients to send to
    private HashMap<String,List<JSON.MarketDataSubscriptionRequestMessage>> instructionIdSubMap
            = new HashMap<String,List<JSON.MarketDataSubscriptionRequestMessage>>( );
    // ecn_instrument:instructionId
    // used when a new sub arrives to figure out if we're already subbed for that instrument
    private HashMap<String,String>  ecnInstInstructionIdMap = new HashMap<String,String>( );

    private HashMap<String,JSON.VenueSessionIdMessage>  sessionIdMap = new HashMap<String,JSON.VenueSessionIdMessage>( );

    private TransficcService service = null;

    // need an executor for the thread that will intermittently send data to the client
    private ExecutorService executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ticker-processor-%d").build());

    public WebSockPusher( Properties props, LinkedBlockingQueue<JSON.Message> outq, TransficcService tfs)
    {
        String liquidityEdgeInsId = props.getProperty( "LiquidityEdgeInstructionId");
        String iswapInsId = props.getProperty( "IswapInstructionId");
        // NB we're making the ECN names all lower case to simplify matching with
        // client subscription requests.
        venueInstructionIdMap.put( iswapInsId, "iswap");
        venueInstructionIdMap.put( liquidityEdgeInsId, "liquidityedge");
        outQ = outq;
        service = tfs;
        logr.info( "WebSockPusher.ctor");
    }

    @Override
    public String call( ) throws Exception
    {
        JSON.Message msg = null;
        while (keepRunning.get())
        {
            msg = outQ.take();
            if (msg.MessageType.equals( "MarketDataUpdateMessage"))
            {
                onMarketDataUpdate((JSON.MarketDataUpdateMessage)msg);
            }
            else if (msg.MessageType.equals( "VenueSessionIdMessage"))
            {
                onVenueSessionId((JSON.VenueSessionIdMessage)msg);
            }
            else if (msg.MessageType.equals( "MarketDataSubscriptionRequestMessage"))
            {
                onMarketDataSubscriptionRequest((JSON.MarketDataSubscriptionRequestMessage)msg);
            }
            else if (msg.MessageType.equals( "ClientDisconnectMessage"))
            {
                onClientDisconnect((JSON.ClientDisconnectMessage)msg);
            }
        }
        return "done";
    }

    private void onMarketDataUpdate( JSON.MarketDataUpdateMessage upd)
    {
        if ( !instructionIdSubMap.containsKey( upd.instructionId)) {
            logr.warning( "WebSockPusher.onMarketDataUpdate: unknown instructionId: " + upd.instructionId);
            return;
        }
        // Send the update to all subscribers...
        List<JSON.MarketDataSubscriptionRequestMessage> reqlist = instructionIdSubMap.get( upd.instructionId);
        TextWebSocketFrame frame = null;
        String jmsg = null;
        for ( JSON.MarketDataSubscriptionRequestMessage req : reqlist) {
            if (jmsg == null) {
                JSON.MarketDataPublishMessage pmsg = new JSON.MarketDataPublishMessage(upd, req.ecn, req.instrumentId);
                jmsg = gson.toJson( pmsg);
                logr.info( "WebSockPusher.onMarketDataUpdate: subscribers(" + reqlist.size( ) + ") " + jmsg);
            }
            logr.info( "WebSockPusher.onMarketDataUpdate: pushing to " + req.channel.toString( ));
            frame = new TextWebSocketFrame( jmsg);
            req.channel.writeAndFlush( frame);
        }
    }

    private void onVenueSessionId( JSON.VenueSessionIdMessage sid)
    {
        if ( !venueInstructionIdMap.containsKey( sid.instructionId)) {
            logr.warning( "WebSockPusher.onVenueSessionId: unknown instructionId: " + sid.instructionId);
            return;
        }
        String ecn = venueInstructionIdMap.get( sid.instructionId);
        logr.info( "WebSockPusher.onVenueSessionId: ecn(" + ecn + ") instructionId("
                   + sid.instructionId + ") sid.sessionId(" + sid.sessionId + ")");
        sessionIdMap.put( ecn, sid);
    }

    private void onMarketDataSubscriptionRequest( JSON.MarketDataSubscriptionRequestMessage sreq)
    {
        String ecn = sreq.ecn.toLowerCase( );
        if ( !sessionIdMap.containsKey( ecn)) {
            logr.info( "WebSockPusher.onMarketDataSubscriptionRequest: no sessionIdMap entry for: " + ecn);
            return;
        }
        String instrumentInstructionId = null;
        List<JSON.MarketDataSubscriptionRequestMessage> reqlist = null;
        // ecnInstInstructionIdMap
        // sessionIdMap keys come from instructionIdMap values, which are lower case.
        // So convert the ECN in the mkt data sub req to lower case to match.
        JSON.VenueSessionIdMessage sid = sessionIdMap.get( ecn);
        String ecn_inst = ecn + "_" + sreq.instrumentId;
        if ( ecnInstInstructionIdMap.containsKey( ecn_inst)) {
            // We've already got a sub for this instrument. Let's check to see if this client
            // is on the subscriber list.
            instrumentInstructionId = ecnInstInstructionIdMap.get( ecn_inst);
            reqlist = instructionIdSubMap.get( instrumentInstructionId);
            for ( JSON.MarketDataSubscriptionRequestMessage req : reqlist) {
                if ( req.equals( sreq)) {
                    // yes - this client already subscribed, so bail out.
                    logr.info( "WebSockPusher.onMarketDataSubscriptionRequest: client("
                               + sreq.channel + ") already subscribed for " + ecn_inst);
                    return;
                }
            }
            // Some other client already subbed to this instrument, so add this client to the sub list.
            logr.info( "WebSockPusher.onMarketDataSubscriptionRequest: new client("
                       + sreq.channel + ") subscribed for " + ecn_inst);
            reqlist.add( sreq);
            return;
        }
        // This is the first sub to this instrument
        reqlist = new ArrayList<JSON.MarketDataSubscriptionRequestMessage>( );
        reqlist.add( sreq);
        instrumentInstructionId = instructionIdGenerator.generateInstructionId( );
        instructionIdSubMap.put( instrumentInstructionId, reqlist);
        ecnInstInstructionIdMap.put( ecn_inst, instrumentInstructionId);
        service.subscribeToMarketData( sid.sessionId, instrumentInstructionId, sreq.instrumentId);
        logr.info( "WebSockPusher.onMarketDataSubscriptionRequest: new sub(" + instrumentInstructionId +
                   ") new client(" + sreq.channel + ") subscribed for " + ecn_inst);
    }

    private void onClientDisconnect( JSON.ClientDisconnectMessage dc)
    {
        // Can't change a list while iterating over it, so we use rmlist to gather the subs to be removed
        List<JSON.MarketDataSubscriptionRequestMessage> reqlist = null;
        List<JSON.MarketDataSubscriptionRequestMessage> rmlist = new ArrayList<JSON.MarketDataSubscriptionRequestMessage>( );
        Iterator it = instructionIdSubMap.entrySet( ).iterator( );
        int rmcount = 0;
        while ( it.hasNext( )) {
            Map.Entry pair = ( Map.Entry)it.next( );
            reqlist = ( List<JSON.MarketDataSubscriptionRequestMessage>)pair.getValue( );
            for ( JSON.MarketDataSubscriptionRequestMessage req : reqlist) {
                if ( req.channel.equals( dc.channel)) {
                    rmlist.add( req);
                    rmcount++;
                }
            }
            for ( JSON.MarketDataSubscriptionRequestMessage req : rmlist) {
                reqlist.remove( req);
            }
            rmlist.clear( );
        }
        logr.info( "WebSockPusher.onClientDisconnect: client(" + dc.channel + ") "
                   + rmcount + " subscriptions removed");
    }
}
