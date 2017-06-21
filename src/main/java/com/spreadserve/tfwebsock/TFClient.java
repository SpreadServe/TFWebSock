package com.spreadserve.tfwebsock;

import com.google.gson.Gson;
import com.transficc.client.library.publiclayer.Environment;
import com.transficc.client.library.publiclayer.LogonStatus;
import com.transficc.client.library.publiclayer.OrderStatus;
import com.transficc.client.library.publiclayer.RFQTradingStatus;
import com.transficc.client.library.publiclayer.Side;
import com.transficc.client.library.publiclayer.TransficcCallback;
import com.transficc.client.library.publiclayer.Venue;
import com.transficc.client.library.publiclayer.VenueLogonStatus;
import com.transficc.client.library.publiclayer.VenueNotificationsCallback;
import com.transficc.client.library.publiclayer.VenueType;
import com.transficc.client.library.publiclayer.instructions.LastLookInstruction;
import com.transficc.client.library.publiclayer.reports.InstructionUpdate;
import com.transficc.client.library.publiclayer.reports.MarketDataUpdate;
import com.transficc.client.library.publiclayer.reports.RFQTradeUpdateReport;
import com.transficc.client.library.publiclayer.reports.RequestForQuote;
import com.transficc.client.library.publiclayer.reports.StatusUpdateReport;
import com.transficc.client.library.publiclayer.reports.TradeUpdateReport;
import com.transficc.client.library.transport.inbound.VenueLogoutStatus;
import com.transficc.sample.inbound.StreamingNotifications;
import com.transficc.sample.outbound.TransficcService;
import com.transficc.sample.service.ServiceFactory;
import com.transficc.sample.util.InstructionIdGenerator;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.transficc.sample.service.ServiceFactory.TransficcNotificationsFactory;


class TFClient implements Callable<String> {
    private static Logger logr = LoggerFactory.getLogger( TFClient.class);
    private static String LE_LON_INSTR_ID;
    private static String I_SWAP_LOGON_INSTRUCTION_ID;

    private LinkedBlockingQueue<JSON.Message>   outQ;
    private Gson                                gson;
    private Properties                          props;
    private TransficcService                    service;

    boolean running = false;
    int timeout;

    public static final class VenueNotificationsFactoryImpl implements ServiceFactory.VenueNotificationsFactory {
        private LinkedBlockingQueue<JSON.Message> outQ;
        public VenueNotificationsFactoryImpl( LinkedBlockingQueue<JSON.Message> outQ) {
            this.outQ = outQ;
        }
        public VenueNotificationsCallback createHandler(TransficcService transficcService) {
            return new VenueNotificationsHandler( transficcService, outQ);
        }
    }

    public static final class TransficcNotificationsFactoryImpl implements TransficcNotificationsFactory {
        private Properties props;
        public TransficcNotificationsFactoryImpl( Properties props) { this.props = props; }
        public TransficcCallbackImpl create( TransficcService transficcService) {
            return new TransficcCallbackImpl( transficcService, props);
        }
    }

    public TFClient( Properties props, LinkedBlockingQueue<JSON.Message> outq)
    {
        LE_LON_INSTR_ID = props.getProperty( "LiquidityEdgeInstructionId");
        I_SWAP_LOGON_INSTRUCTION_ID = props.getProperty( "IswapInstructionId");
        this.props = props;
        outQ = outq;
        gson = new Gson( );
        running = true;

        final TransficcNotificationsFactory transficcNotificationsFactory = new TransficcNotificationsFactoryImpl( props);
        final ServiceFactory.VenueNotificationsFactory venueNotificationsFactory = new VenueNotificationsFactoryImpl(outQ);
        final ServiceFactory.VenueStreamingNotificationsFactory venueStreamingNotificationsFactory = (service) -> new StreamingNotifications();
        service = ServiceFactory.createService(Environment.DEMO, transficcNotificationsFactory, venueNotificationsFactory, venueStreamingNotificationsFactory);
    }

    public TransficcService GetService( )
    {
        return service;
    }

    public String call( )
    {
        String uid = this.props.getProperty( "TransficcUserId");
        String pwd = this.props.getProperty( "TransficcPassword");
        logr.info( "TFClient.call: logging in with TransficcUserId: " + uid);
        service.transficcLogon( uid, pwd);
        return "";
    }

    private static final class TransficcCallbackImpl implements TransficcCallback
    {
        private final TransficcService service;
        private Properties props;

        private TransficcCallbackImpl( final TransficcService service, Properties props)
        {
            this.service = service;
            this.props = props;
        }

        @Override
        public void onUnauthenticatedRequest()
        {
        }

        @Override
        public void onLogon( final LogonStatus logonStatus)
        {
            // service.venueLogon(LE_LON_INSTR_ID, Venue.LIQUIDITY_EDGE, VenueType.MARKET_DATA, Credentials.LIQUIDITY_EDGE_MARKET_DATA.getUsername(), Credentials.LIQUIDITY_EDGE_MARKET_DATA.getPassword());
            String iswapInstructionId = props.getProperty( "IswapInstructionId");
            String iswapUserId = props.getProperty( "IswapUserId");
            String iswapPassword = props.getProperty( "IswapPassword");
            logr.info( "TFClient.onLogon: TransFICC LogonStatus(" + logonStatus + ") logging on to Iswap as user("
                                                + iswapUserId + ") with instructionId(" + iswapInstructionId + ")");
            service.venueLogon(iswapInstructionId, Venue.ISWAP, VenueType.MARKET_DATA, iswapUserId, iswapPassword);
        }
    }

    private static final class VenueNotificationsHandler implements VenueNotificationsCallback
    {
        private static final String BLUE = (char)27 + "[34m";
        private static final String MAGENTA = (char)27 + "[35m";
        private final InstructionIdGenerator instructionIdGenerator = new InstructionIdGenerator();
        private final TransficcService service;
        private long leSessionId;
        private long iswapSessionId;
        private LinkedBlockingQueue<JSON.Message> outQ;
        private JSON json = new JSON( );

        private VenueNotificationsHandler(final TransficcService service, LinkedBlockingQueue<JSON.Message> outQ)
        {
            this.service = service;
            this.outQ = outQ;
        }

        @Override
        public void onLogon(final CharSequence cInstructionId, final long venueSessionId, final VenueLogonStatus logonStatus)
        {
            String instructionId = cInstructionId.toString( );
            logr.info( "VenueNotificationsHandler.onLogon: LogonStatus(" + logonStatus + ") sessionId("
                       + venueSessionId + ") with instructionId(" + instructionId + ")");
            JSON.VenueSessionIdMessage msg = new JSON.VenueSessionIdMessage( instructionId, venueSessionId);
            try {
                outQ.put( msg);
            }
            catch (InterruptedException e) {
                logr.info( "TFClient.VenueNotificationsHandler.onLogon: outQ.put( ) failed "
                           + json.ToJsonString( msg) + "\n" + e.toString( ));
            }
        }

        @Override
        public void onLogout(final long venueSessionId, final VenueLogoutStatus reason)
        {
        }

        @Override
        public void onRequestForQuote(final long venueSessionId, final RequestForQuote requestForQuote)
        {
        }

        @Override
        public void onInstructionUpdate(final long venueSessionId, final InstructionUpdate instructionUpdate)
        {
            logr.info( "VenueNotificationsHandler.onInstructionUpdate: sessionId(" + venueSessionId
                          + ") " + instructionUpdate.toString( ));
        }

        @Override
        public void onStatusUpdate(final long venueSessionId, final StatusUpdateReport statusUpdateReport)
        {
        }

        @Override
        public void onTradeUpdate(final long venueSessionId, final TradeUpdateReport tradeUpdateReport)
        {
        }

        @Override
        public void onLastLookInstruction(final long venueSessionId, final LastLookInstruction lastLookInstruction)
        {
        }

        @Override
        public void onOrderLimitExceeded(final long venueSessionId)
        {
        }

        @Override
        public void onMarketDataUpdate(final long venueSessionId, final MarketDataUpdate marketDataUpdate)
        {
            if ( marketDataUpdate.getSide( ) == Side.NOT_SET)
                return;
            JSON.MarketDataUpdateMessage upd = new JSON.MarketDataUpdateMessage( venueSessionId, marketDataUpdate);
            try {
                outQ.put( upd);
            }
            catch (InterruptedException e) {
                logr.info("TFClient.VenueNotificationsHandler.onMarketDataUpdate: outQ.put( ) failed "
                          + json.ToJsonString(upd) + "\n" + e.toString());
            }
        }

        @Override
        public void onRFQTradeUpdate(final long l, final RFQTradeUpdateReport rfqTradeUpdateReport)
        {
        }

        @Override
        public void onEndOfRFQNegotiation(long venueSessionId, CharSequence venueAssignedId, RFQTradingStatus status)
        {}

        @Override
        public void onOrderStatus(long venueSessionId, CharSequence instructionId, OrderStatus orderStatus)
        {}
    }

}

