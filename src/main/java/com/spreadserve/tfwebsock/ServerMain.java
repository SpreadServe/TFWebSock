package com.spreadserve.tfwebsock;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.transficc.sample.outbound.TransficcService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TransFICC WebSocketServer. (c) SpreadServe Ltd 2017
 * Clients are  spreadsheets using SSAddin.xll that accept the path of a file an echo back its content.
 * This server is in turn a client of TransFICC APIs and services.
 */
public final class ServerMain
{

    private static final Logger logger = LoggerFactory.getLogger( WebSocketHandler.class);

    public static void main(String[] args) throws Exception {
        // must do this first for logback
        InternalLoggerFactory.setDefaultFactory( new Slf4JLoggerFactory( ));

        // args[0] should be the path of the transficc.props properties file
        String propsFilePath = args[0];
        Properties props = new Properties( );

        // load up the props for socket and DB config
        File pfile = new File( propsFilePath);
        logger.info( "Loading config from: " + propsFilePath);
        InputStream istrm = new FileInputStream( propsFilePath);
        props.load( istrm);
        String sport = props.getProperty( "SocketServerPort", "8080");
        logger.info( "Binding to port: " + sport);
        int port = Integer.parseInt( sport);

        // Create the transficc client, and the Q it uses to send events including
        // market data back to the thread that pushes updates on the web sockets.
        // Also an executor to provide a thread to run the TFClient. We keep a ref
        // to the transficc service so we can pass it to the web socket handler.
        LinkedBlockingQueue<JSON.Message> outQ = new LinkedBlockingQueue<JSON.Message>( );
        TFClient tfc = new TFClient( props, outQ);
        TransficcService tfs = tfc.GetService( );
        // need an executor for the thread that will intermittently send data to the client
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder( );
        builder.setDaemon( true);
        builder.setNameFormat( "transficc-%d");
        ExecutorService transficcExecutor = Executors.newSingleThreadExecutor( builder.build( ));
        FutureTask<String> transficcTask = new FutureTask<String>( tfc);
        transficcExecutor.execute( transficcTask);

        // Now we can create the pusher object and thread, which consumes the event
        // on the outQ. Those events originate with TFClient callbacks from transficc,
        // and also incoming websock events like subscription requests.
        WebSockPusher pusher = new WebSockPusher( props, outQ, tfs);
        ExecutorService pusherExecutor = Executors.newSingleThreadExecutor( builder.build( ));
        FutureTask<String> pusherTask = new FutureTask<String>( pusher);
        pusherExecutor.execute( pusherTask);

        // Compose the server components...
        EventLoopGroup bossGroup = new NioEventLoopGroup( 1);
        EventLoopGroup workerGroup = new NioEventLoopGroup( );
        try {
            ServerBootstrap b = new ServerBootstrap( );
            LoggingHandler lh = new LoggingHandler(LogLevel.INFO);
            ChannelInitializer ci = new ChannelInitializer<SocketChannel>( ) {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast("encoder", new HttpResponseEncoder());
                    p.addLast("decoder", new HttpRequestDecoder());
                    p.addLast("aggregator", new HttpObjectAggregator(65536));
                    p.addLast("handler", new WebSocketHandler( props, outQ));
                }
            };
            b.group( bossGroup, workerGroup).channel( NioServerSocketChannel.class).handler( lh).childHandler( ci);
            // Fire up the server...
            ChannelFuture f = b.bind( port).sync( );
            logger.info( "Server started");
            // Wait until the server socket is closed.
            f.channel( ).closeFuture( ).sync( );
        }
        finally {
            logger.info( "Server shutdown started");
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully( );
            workerGroup.shutdownGracefully();
            logger.info( "Server shutdown completed");
        }
    }
}