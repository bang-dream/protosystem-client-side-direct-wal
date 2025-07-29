package hs.jjm;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simulated a database server which receives the WAL Entry as a write request,
 * then inserts the WAL Entry into the database (by simulated actions), where a
 * globally monotonically increasing p-sn is generated and returned to the client.
 */
public class SimulatedDatabaseServer {
  private static final int PORT = 10086;
  private static final int HANDLER_THREADS_NUM = 100;
  private static final AtomicLong P_SN_GENERATOR = new AtomicLong(0);
  private static final ExecutorService HANDLER_POOL = Executors.newFixedThreadPool(HANDLER_THREADS_NUM);

  public static void main(String[] args) throws Exception {
    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();

    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();

              // Add frame decoder/encoder for handling length fields
              pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
              pipeline.addLast(new LengthFieldPrepender(4));

              // Add custom handlers
              pipeline.addLast(new WalEntryDecoder());
              pipeline.addLast(new PcpMapMsgEncoder());
              pipeline.addLast(new WalEntryProcessor());
            }
          })
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true);

      ChannelFuture f = b.bind(PORT).sync();
      System.out.println("Server started and listening on port " + PORT);
      f.channel().closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      HANDLER_POOL.shutdown();
    }
  } // Main method.

  private static class WalEntryDecoder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof byte[]) {
        byte[] bytes = (byte[]) msg;
        try {
          WALEntry entry = WALEntry.deserialize(bytes);
          ctx.fireChannelRead(entry);
        } catch (IllegalArgumentException e) {
          System.err.println("Invalid WALEntry received: " + e.getMessage());
          ctx.close();
        }
      } else {
        super.channelRead(ctx, msg);
      }
    }
  }

  private static class PcpMapMsgEncoder extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof PCpMapMsg) {
        byte[] bytes = ((PCpMapMsg) msg).serialize();
        super.write(ctx, bytes, promise);
      } else {
        super.write(ctx, msg, promise);
      }
    }
  }

  private static class WalEntryProcessor extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof WALEntry) {
        final WALEntry entry = (WALEntry) msg;
        final ChannelHandlerContext context = ctx;
        final Handler handler = new Handler(entry, context);
        HANDLER_POOL.submit(handler);
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      System.err.println("Error in processing: " + cause.getMessage());
      ctx.close();
    }
  }

  private static class Handler implements Runnable {
    private final WALEntry writeRequest;
    private final ChannelHandlerContext context;

    private Handler(WALEntry writeRequest, ChannelHandlerContext context) {
      this.writeRequest = writeRequest;
      this.context = context;
    }

    @Override
    public void run() {
      // Generate monotonically increasing P-SN
      long pSn = P_SN_GENERATOR.incrementAndGet();
      long cpSn = writeRequest.getCpSn();

      // Simulated handling.
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        System.err.println("Handler.run: Interrupted");
      }

      // Create and send response
      PCpMapMsg response = new PCpMapMsg(pSn, cpSn);
      context.writeAndFlush(response);
    }
  }
}
