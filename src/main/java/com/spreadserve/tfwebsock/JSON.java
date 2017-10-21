package com.spreadserve.tfwebsock;

import java.io.StringReader;
import java.util.HashMap;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.transficc.client.library.publiclayer.types.Side;
import com.transficc.client.library.publiclayer.inbound.MarketDataUpdate;

/*  A collection of POJOs used on the message Q that connects the
    transficc thread and the socket push thread. The transficc thread
    is running the TFClient code, and the socket push is in WebSockPusher.
  */
public class JSON {

    private static Logger logr = LoggerFactory.getLogger( JSON.class);

    private HashMap<String,Class> typeMap;
    private Gson gson;

    JSON( ) {
        gson = new Gson( );
        typeMap = new HashMap<String,Class>( );
        typeMap.put( "Message", Message.class);
        typeMap.put( "MarketDataUpdate", MarketDataUpdate.class);
        typeMap.put( "MarketDataPublishMessage", MarketDataPublishMessage.class);
        typeMap.put( "VenueSessionIdMessage", VenueSessionIdMessage.class);
        typeMap.put( "MarketDataInstructionIdMessage", MarketDataInstructionIdMessage.class);
        typeMap.put( "MarketDataSubscriptionRequestMessage", MarketDataSubscriptionRequestMessage.class);
        typeMap.put( "ClientDisconnectMessage", ClientDisconnectMessage.class);
    }

    Message ToObject( String json) {
        JsonReader reader = new JsonReader( new StringReader( json));
        reader.setLenient( true);
        Message msg = gson.fromJson( reader, Message.class);
        if ( msg == null) {
            logr.info( "JSON marshalling to base Message failed for " + json);
            return null;
        }
        Class mtype = typeMap.get( msg.MessageType);
        if ( mtype == null) {
            logr.info( "JSON marshalling to " + msg.MessageType + " failed for " + json);
            return null;
        }
        // we have to create a new reader for the second pass...
        reader = new JsonReader( new StringReader( json));
        reader.setLenient( true);
        msg = ( Message)gson.fromJson( reader, mtype);
        return msg;
    }

    String ToJsonString( Message msg) {
        return gson.toJson( msg);
    }

    public static class Message {
        public String MessageType;
        public Message( ) {
            MessageType=this.getClass( ).getSimpleName( );
        }
    }

    // MarketDataUpdateMessage: for passing updates from transficc thread to websocket worker thread
    public static class MarketDataUpdateMessage extends Message {
        public String                   instructionId;
        public Side                     side;
        public double                   quantity;
        public double                   price;
        public MarketDataUpdate.Action  action;
        public long                     sessionId;
        public MarketDataUpdateMessage( long sessId, MarketDataUpdate upd) {
            sessionId = sessId;
            instructionId = upd.getInstructionId( ).toString( );
            side = upd.getSide( );
            quantity = upd.getQuantity( );
            price = upd.getPrice( );
            action = upd.getAction( );
        }
    }

    // MarketDataPublishMessage: for publishing market data from the websocket worker thread
    public static class MarketDataPublishMessage extends Message {
        public String                   instructionId;
        public Side                     side;
        public double                   quantity;
        public double                   price;
        public MarketDataUpdate.Action  action;
        public long                     sessionId;
        public String                   ecn;
        public String                   instrumentId;
        public MarketDataPublishMessage( MarketDataUpdateMessage upd, String ecn, String inst) {
            sessionId = upd.sessionId;
            instructionId = upd.instructionId;
            side = upd.side;
            quantity = upd.quantity;
            price = upd.price;
            action = upd.action;
            this.ecn = ecn;
            instrumentId = inst;
        }
    }

    // VenueSessionIdMessage: transficc fires a callback after login with the sessionId
    // This message is passed to the websocket worker thread
    public static class VenueSessionIdMessage extends Message {
        public String   instructionId;
        public long     sessionId;
        public VenueSessionIdMessage( String insId, long sessId) {
            instructionId = insId;
            sessionId = sessId;
        }
    }

    public static class MarketDataInstructionIdMessage extends Message {
        public String   instructionId;
        public String   instrumentId;
        public MarketDataInstructionIdMessage( String instructId, String instrumentId) {
            this.instructionId = instructId;
            this.instrumentId = instrumentId;
        }
    }

    public static class MarketDataSubscriptionRequestMessage extends Message {
        public String   ecn;
        public String   instrumentId;
        public Channel  channel;
        public MarketDataSubscriptionRequestMessage( String ecn, String instrumentId, Channel chan) {
            this.ecn = ecn;
            this.instrumentId = instrumentId;
            this.channel = chan;
        }
    }

    public static class ClientDisconnectMessage extends Message {
        public Channel  channel;
        public ClientDisconnectMessage( Channel chan) {
            this.channel = chan;
        }
    }
}
