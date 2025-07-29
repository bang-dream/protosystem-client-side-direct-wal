package hs.jjm;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * A simulated client which constantly generate WALEntries (also treated as write requests), and
 * concurrently write the WALEntries to the HDFS and simulated database server. When responses from
 * both the HDFS and simulated database server are received, treat the write request as finished.
 */
public class SimulatedClient {
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 10086;
  private static final String HDFS_PATH = "/wal_entries/wal.log";
  private static final AtomicLong CP_SN_GENERATOR = new AtomicLong(0);
  private static final ScheduledExecutorService CLIENT_SCHEDULER = Executors.newScheduledThreadPool(4);
  private static final ExecutorService WAL_WRITER_POOL = Executors.newFixedThreadPool(2);
  private static final ExecutorService PCP_WRITER_POOL = Executors.newFixedThreadPool(2);

  private static FanOutOneBlockAsyncDFSOutput HDFS_OUT;
  private static final Object HDFS_LOCK = new Object();

  private static final AtomicLong WAL_NUMS_IN_FLY = new AtomicLong(0);

  // Key => cpSn, Value => InFlyCtx.
  private static final ConcurrentMap<Long, InFlyCtx> IN_FLY_REQUEST = new ConcurrentHashMap<>();

  private static class InFlyCtx {
    long startWriteTimeNanos = System.nanoTime();
    long returnableTimeNanos = -1;
    boolean hdfsWalFlushed = false;
    boolean simulatedDbFinished = false;
    boolean hdfsPcPMsgFlushed = false;
  }

  public static void main(String[] args) throws Exception {
    // Initialize HDFS
    initHdfs();

    // Configure Netty client
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(group)
          .channel(NioSocketChannel.class)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();

              // Frame handlers
              pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
              pipeline.addLast(new LengthFieldPrepender(4));

              // Custom handlers
              pipeline.addLast(new WalEntryEncoder());
              pipeline.addLast(new PcpMapMsgDecoder());
              pipeline.addLast(new ClientHandler());
            }
          });

      // Connect to server
      ChannelFuture f = b.connect(SERVER_HOST, SERVER_PORT).sync();
      System.out.println("Connected to server at " + SERVER_HOST + ":" + SERVER_PORT);

      // Start client threads
      startClientHandlers(f.channel());

      // Wait until connection is closed
      f.channel().closeFuture().sync();
    } finally {
      group.shutdownGracefully();
      CLIENT_SCHEDULER.shutdown();
      WAL_WRITER_POOL.shutdown();
      PCP_WRITER_POOL.shutdown();
      closeHdfs();
    }
  }

  private static void initHdfs() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(HDFS_PATH);

    org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup group = new org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup();

    HDFS_OUT = FanOutOneBlockAsyncDFSOutputHelper.createOutput(
        (DistributedFileSystem) fs,
        path,
        false,
        true,
        (short) 1,
        1024 * 1024 * 1024,
        group,
        org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel.class,
        new StreamSlowMonitor(conf, "unmane", new ExcludeDatanodeManager(conf)),
        false);
  }

  private static void closeHdfs() {
    synchronized (HDFS_LOCK) {
      if (HDFS_OUT != null) {
        try {
          HDFS_OUT.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void startClientHandlers(Channel channel) {
    // Start 3 client handlers that periodically send WALEntries
    for (int i = 0; i < 3; i++) {
      CLIENT_SCHEDULER.scheduleAtFixedRate(() -> {
        long cpSn = CP_SN_GENERATOR.incrementAndGet();
        byte[] content = ("WALEntry content for cpSn=" + cpSn).getBytes();
        WALEntry entry = new WALEntry(cpSn, content);
        IN_FLY_REQUEST.put(cpSn, new InFlyCtx());

        // Send to server
        channel.writeAndFlush(entry);

        // Submit to WAL writer
        WAL_WRITER_POOL.submit(new WALWriteHandler(entry));

        WAL_NUMS_IN_FLY.incrementAndGet();
      }, 0, 10, TimeUnit.MILLISECONDS); // Tunnel this parameter.
    }
  }

  private static class WalEntryEncoder extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof WALEntry) {
        byte[] bytes = ((WALEntry) msg).serialize();
        super.write(ctx, bytes, promise);
      } else {
        super.write(ctx, msg, promise);
      }
    }
  }

  private static class PcpMapMsgDecoder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof byte[]) {
        byte[] bytes = (byte[]) msg;
        try {
          PCpMapMsg pcpMsg = new PCpMapMsg(0, 0).deserialize(bytes);
          System.out.println("Received PCpMapMsg: pSn=" + pcpMsg.getPSn() + ", cpSn=" + pcpMsg.getCpSn());

          // Submit to PCp writer
          PCP_WRITER_POOL.submit(new PCpMsgWriteHandler(pcpMsg));
        } catch (IllegalArgumentException e) {
          System.err.println("Invalid PCpMapMsg received: " + e.getMessage());
        }
      } else {
        super.channelRead(ctx, msg);
      }
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      System.err.println("Client error: " + cause.getMessage());
      ctx.close();
    }
  }

  private static class WALWriteHandler implements Runnable {
    private final WALEntry entry;

    public WALWriteHandler(WALEntry entry) {
      this.entry = entry;
    }

    @Override
    public void run() {
      synchronized (HDFS_LOCK) {
        System.out.println("Writing WALEntry to HDFS: cpSn=" + entry.getCpSn());
        HDFS_OUT.write(entry.serialize());
        CompletableFuture<Long> future =  HDFS_OUT.flush(false); // async protocol.
        future.whenComplete(new BiConsumer<Long, Throwable>() {
          @Override
          public void accept(Long aLong, Throwable throwable) {
            if (throwable == null) {
              System.out.println("Writing WALEntry to HDFS finished: cpSn=" + entry.getCpSn());
              InFlyCtx inFlyContext = IN_FLY_REQUEST.get(entry.getCpSn());
              inFlyContext.hdfsWalFlushed = true;

              // This may cause a concurrent issue. But this system is just for test, ignore it.
              if (inFlyContext.simulatedDbFinished) {
                inFlyContext.returnableTimeNanos = System.nanoTime();
              }

              // Time for record metrics.
              long elapsedTimeNanos = System.nanoTime() - inFlyContext.startWriteTimeNanos;

            }
          }
        });
      }
    }
  }

  private static class PCpMsgWriteHandler implements Runnable {
    private final PCpMapMsg pcpMsg;

    public PCpMsgWriteHandler(PCpMapMsg pcpMsg) {
      this.pcpMsg = pcpMsg;
    }

    @Override
    public void run() {
      synchronized (HDFS_LOCK) {
        System.out.println("Writing PCpMapMsg to HDFS: pSn=" + pcpMsg.getPSn() + ", cpSn=" + pcpMsg.getCpSn());

        InFlyCtx inFlyContext = IN_FLY_REQUEST.get(pcpMsg.getCpSn());
        inFlyContext.hdfsPcPMsgFlushed = true;

        // This may cause a concurrent issue. But this system is just for test, ignore it.
        if (inFlyContext.hdfsWalFlushed) {
          inFlyContext.returnableTimeNanos = System.nanoTime();
        }

        // Time for record metrics.
        long elapsedTimeNanos = System.nanoTime() - inFlyContext.startWriteTimeNanos;

        HDFS_OUT.write(pcpMsg.serialize());
        CompletableFuture<Long> future = HDFS_OUT.flush(false); // async protocol.
        future.whenComplete(new BiConsumer<Long, Throwable>() {
          @Override
          public void accept(Long aLong, Throwable throwable) {
            if (throwable == null) {
              System.out.println("Writing PCpMapMsg to HDFS finished: pSn=" + pcpMsg.getPSn() + ", cpSn=" + pcpMsg.getCpSn());
              InFlyCtx inFlyContext = IN_FLY_REQUEST.remove(pcpMsg.getCpSn());

              // Time for record metrics.
              long elapsedTimeNanos = System.nanoTime() - inFlyContext.startWriteTimeNanos;

              WAL_NUMS_IN_FLY.decrementAndGet();
            }
          }
        });
      }
    }
  }
}
