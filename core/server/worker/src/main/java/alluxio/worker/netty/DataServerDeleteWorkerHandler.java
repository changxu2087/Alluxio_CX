package alluxio.worker.netty;

import alluxio.RpcUtils;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.file.FileSystemContext;
import alluxio.client.netty.NettyRPC;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Created by shawn-pc on 10/13/17. Netty handler that handles delete worker requests.
 */
@NotThreadSafe
public class DataServerDeleteWorkerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerDeleteWorkerHandler.class);

  private static final long INVALID_SESSION_ID = -1;

  /** The block worker. */
  private final BlockWorker mBlockWorker;

  private long mSessionId = INVALID_SESSION_ID;

  /**
   * Creates an instance of {@link DataServerDeleteWorkerHandler}.
   *
   * @param blockWorker the block worker
   */
  DataServerDeleteWorkerHandler(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    LOG.debug("get the message");
    System.out.println("get the message");
    if (!(msg instanceof RPCProtoMessage)) {
      ctx.fireChannelRead(msg);
      return;
    }
    ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
    if (message.isDeleteWorkerRequest()) {
      LOG.debug("is delete worker request");
      System.out.println("is delete worker request");
      handleDeleteWorkerRequest(ctx, message.asDeleteWorkerRequest());
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
    // The RPC handlers do not throw exceptions. All the exception seen here is either
    // network exception or some runtime exception (e.g. NullPointerException).
    LOG.error("Failed to handle RPCs.", throwable);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    if (mSessionId != INVALID_SESSION_ID) {
      mBlockWorker.cleanupSession(mSessionId);
      mSessionId = INVALID_SESSION_ID;
    }
    ctx.fireChannelUnregistered();
  }

  /**
   * Handles {@link Protocol.DeleteWorkerRequest} to delete worker. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the delete worker request
   */
  private void handleDeleteWorkerRequest(final ChannelHandlerContext ctx,
      final Protocol.DeleteWorkerRequest request) {
    RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

      @Override
      public Void call() throws Exception {
        if (mSessionId == INVALID_SESSION_ID) {
          LOG.debug("gogogo");
          System.out.println("gogogo");
          mSessionId = IdUtils.createSessionId();
          long transferByte = request.getTranferByte();
          String availableWorker = request.getAvailableWorkerAddress();
          System.out.println("deleting worker " + availableWorker + " " + transferByte);
          mBlockWorker.deleteWorker(mSessionId, availableWorker, transferByte);
          mSessionId = INVALID_SESSION_ID;
          System.out.println("deleted worker");
        } else {
          LOG.warn("Delete worker without closing the previous session {}.", mSessionId);
          throw new InvalidWorkerStateException(
              ExceptionMessage.SESSION_NOT_CLOSED.getMessage(mSessionId));
        }
        return null;
      }

      @Override
      public void exceptionCaught(Throwable throwable) {
        ctx.writeAndFlush(
            RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(throwable)));
        mSessionId = INVALID_SESSION_ID;
      }

      @Override
      public String toString() {
        return String.format("Session %d: delete worker: %s", mSessionId, request.toString());
      }
    });
  }
}
