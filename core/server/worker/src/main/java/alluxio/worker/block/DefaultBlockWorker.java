/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.Sessions;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.options.RemoveWorkerOptions;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.codahale.metrics.Gauge;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Servers: {@link BlockWorkerClientServiceHandler} (RPC Server)
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link DefaultBlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultBlockWorker extends AbstractWorker implements BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockWorker.class);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleaner;

  /** Client for all block master communication. */
  private final BlockMasterClient mBlockMasterClient;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;
  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;
  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions;
  /** Block Store manager. */
  private BlockStore mBlockStore;
  private WorkerNetAddress mAddress;

  /** The under file system block store. */
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;

  /**
   * The worker ID for this worker. This is initialized in {@link #start(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  private AtomicReference<Long> mWorkerId;

  /**
   * Transfer the block of the worker
   */
  private BlockOutStream currentBlockOutStream;
  private List<BlockOutStream> previousBlockOutStream;
  private List<BlockWorkerInfo> availableWorkerInfos;
  private FileWriteLocationPolicy locationPolicy;
  private AlluxioBlockStore mAlluxioBlockStore;

  /**
   * Constructs a default block worker.
   *
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(UfsManager ufsManager) {
    this(new BlockMasterClient(NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC)),
        new FileSystemMasterClient(NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC)),
        new Sessions(), new TieredBlockStore(), ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClient a client for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(BlockMasterClient blockMasterClient,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, BlockStore blockStore,
      UfsManager ufsManager) {
    super(Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mBlockMasterClient = blockMasterClient;
    mFileSystemMasterClient = fileSystemMasterClient;
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mMetricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mBlockStore = blockStore;
    mWorkerId = new AtomicReference<>(-1L);
    mAlluxioBlockStore = AlluxioBlockStore.create();

    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
    mUnderFileSystemBlockStore = new UnderFileSystemBlockStore(mBlockStore, ufsManager);

    Metrics.registerGauges(this);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }

  @Override
  public BlockWorkerClientServiceHandler getWorkerServiceHandler() {
    return new BlockWorkerClientServiceHandler(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME,
        new BlockWorkerClientService.Processor<>(getWorkerServiceHandler()));
    return services;
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   */
  @Override
  public void start(WorkerNetAddress address) throws IOException {
    mAddress = address;
    try {
      mWorkerId.set(mBlockMasterClient.getId(address));
    } catch (Exception e) {
      throw new RuntimeException("Failed to get a worker id from block master: " + e.getMessage());
    }

    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    mBlockMasterSync = new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(this, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleaner = new SessionCleaner(mSessions, mBlockStore, mUnderFileSystemBlockStore);

    // Setup space reserver
    if (Configuration.getBoolean(PropertyKey.WORKER_TIERED_STORE_RESERVER_ENABLED)) {
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER, new SpaceReserver(this),
              Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS)));
    }

    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleaner);
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   */
  @Override
  public void stop() {
    // Steps to shutdown:
    // 1. Gracefully shut down the runnables running in the executors.
    // 2. Shutdown the executors.
    // 3. Shutdown the clients. This needs to happen after the executors is shutdown because
    //    runnables running in the executors might be using the clients.
    mSessionCleaner.stop();
    // The executor shutdown needs to be done in a loop with retry because the interrupt
    // signal can sometimes be ignored.
    CommonUtils.waitFor("block worker executor shutdown", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        getExecutorService().shutdownNow();
        try {
          return getExecutorService().awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    mBlockMasterClient.close();
    mFileSystemMasterClient.close();
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mBlockStore.abortBlock(sessionId, blockId);
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mBlockStore.accessBlock(sessionId, blockId);
  }

  @Override
  public void commitBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    // NOTE: this may be invoked multiple times due to retry on client side.
    // TODO(binfan): find a better way to handle retry logic
    try {
      mBlockStore.commitBlock(sessionId, blockId);
    } catch (BlockAlreadyExistsException e) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure, therefore ignore the exception", blockId, e);
    }

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      mBlockMasterClient.commitBlock(mWorkerId.get(), bytesUsedOnTier, loc.tierAlias(), blockId,
          length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock;
    try {
      createdBlock = mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
    } catch (WorkerOutOfSpaceException e) {
      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new WorkerOutOfSpaceException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e);
    }
    return createdBlock.getPath();
  }

  @Override
  public void createBlockRemote(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
  }

  @Override
  public void freeSpace(long sessionId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.freeSpace(sessionId, availableBytes, location);
  }

  @Override
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    return mBlockStore.getBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return mBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return mBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return mBlockStore.getBlockMeta(sessionId, blockId, lockId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    return mBlockStore.lockBlock(sessionId, blockId);
  }

  @Override
  public long lockBlockNoException(long sessionId, long blockId) {
    return mBlockStore.lockBlockNoException(sessionId, blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // TODO(calvin): Move this logic into BlockStore#moveBlockInternal if possible
    // Because the move operation is expensive, we first check if the operation is necessary
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
    long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mBlockStore.moveBlock(sessionId, blockId, dst);
  }

  @Override
  public String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    return meta.getPath();
  }

  @Override
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  @Override
  public BlockReader readUfsBlock(long sessionId, long blockId, long offset)
      throws BlockDoesNotExistException, IOException {
    return mUnderFileSystemBlockStore.getBlockReader(sessionId, blockId, offset);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mBlockStore.unlockBlock(lockId);
  }

  @Override
  // TODO(calvin): Remove when lock and reads are separate operations.
  public boolean unlockBlock(long sessionId, long blockId) {
    return mBlockStore.unlockBlock(sessionId, blockId);
  }

  @Override
  public void sessionHeartbeat(long sessionId) {
    mSessions.sessionHeartbeat(sessionId);
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return mFileSystemMasterClient.getFileInfo(fileId);
  }

  @Override
  public boolean openUfsBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    return mUnderFileSystemBlockStore.acquireAccess(sessionId, blockId, options);
  }

  @Override
  public void closeUfsBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    mUnderFileSystemBlockStore.closeReaderOrWriter(sessionId, blockId);
    if (mBlockStore.getTempBlockMeta(sessionId, blockId) != null) {
      try {
        commitBlock(sessionId, blockId);
      } catch (BlockDoesNotExistException e) {
        // This can only happen if the session is expired. Ignore this exception if that happens.
        LOG.warn("Block {} does not exist while being committed.", blockId);
      } catch (InvalidWorkerStateException e) {
        // This can happen if there are multiple sessions writing to the same block.
        // BlockStore#getTempBlockMeta does not check whether the temp block belongs to
        // the sessionId.
        LOG.debug("Invalid worker state while committing block.", e);
      }
    }
    mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
  }

  @Override
  public void deleteWorker(long sessionId, String availableWorker, long transferByte) throws IOException {
    currentBlockOutStream = null;
    previousBlockOutStream = new ArrayList<>();
    availableWorkerInfos = new ArrayList<>();
    try {
      locationPolicy =
          CommonUtils.createNewClassInstance(Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    BlockStoreMeta storeMeta = getStoreMetaFull();

    List<BlockWorkerInfo> allBlockWorkerInfos = mAlluxioBlockStore.getWorkerInfoList();
    Set<String> available = new HashSet<>();
    for (String workerHost : availableWorker.split(";")) {
      available.add(workerHost);
    }
    for (BlockWorkerInfo workerInfo : allBlockWorkerInfos) {
      if (available.contains(workerInfo.getNetAddress().getHost())) {
        availableWorkerInfos.add(workerInfo);
      }
    }

    Map<String, List<Long>> blocks = storeMeta.getBlockList();
    List<Long> memBlocks = blocks.get(Configuration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS));
    try {
      System.out.println(memBlocks + " " + transferByte + " transferring block");
      transferBlock(sessionId, memBlocks, transferByte);
      System.out.println(memBlocks + " " + transferByte + " transferred block");
    } finally {
      LOG.debug("stopping the worker");
      System.out.println("stopping the worker");
      stop();
      LOG.debug("stopped the worker");
      System.out.println("stopped the worker");
      LOG.debug("removing the worker");
      System.out.println("removing the worker");
      removeWorker(sessionId, mAddress);
      LOG.debug("removed the worker");
      System.out.println("removed the worker");
    }
  }

  @Override
  public void removeWorker(long sessionId, WorkerNetAddress address) throws IOException {
    FileSystemContext context = FileSystemContext.INSTANCE;
    try (CloseableResource<alluxio.client.block.BlockMasterClient> blockMasterClient =
        context.acquireBlockMasterClientResource()) {
      blockMasterClient.get().removeWorker(address, RemoveWorkerOptions.defaults());
    }
  }

  private void transferBlock(long sessionId, List<Long> blockIds, long transferByte) throws IOException {
    List<Throwable> errors = new ArrayList<>();
    Map<Long, Long> blockIdToLockId = lockBlocks(sessionId, blockIds);
    long haveTransferByte = 0;
    ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
    try (Closer closer = Closer.create()) {
      for (long blockId : blockIds) {
        long lockId = blockIdToLockId.get(blockId);
        long blockSize = getBlockMeta(sessionId, blockId, lockId).getBlockSize();
        if (blockSize + haveTransferByte > transferByte) {
          unlockBlock(lockId);
          continue;
        }
        BlockReader blockReader = readBlockRemote(sessionId, blockId, lockId);
        ReadableByteChannel readableByteChannel = closer.register(blockReader.getChannel());
        while (readableByteChannel.read(buf) != -1) {
          buf.flip();
          writeRemote(blockId, blockSize, buf.array(), 0, buf.limit());
        }
        haveTransferByte += blockSize;
      }
      transforClose();
    } catch (Exception e) {
      errors.add(e);
      for (long lockId : blockIdToLockId.values()) {
        try {
          unlockBlock(lockId);
        } catch (BlockDoesNotExistException e1) {
          errors.add(e1);
        }
      }
      System.out.println("have error");
      handleErrors(errors);
    }
  }

  private Map<Long, Long> lockBlocks(long sessionId, List<Long> blockIds) throws IOException {
    List<Throwable> errors = new ArrayList<>();
    Map<Long, Long> blockIdToLockId = new HashMap<>();
    try {
      // lock all the blocks
      for (long blockId : blockIds) {
        long lockId = lockBlock(sessionId, blockId);
        blockIdToLockId.put(blockId, lockId);
      }
    } catch (BlockDoesNotExistException e) {
      errors.add(e);
      // make sure all the locks are released
      for (long lockId : blockIdToLockId.values()) {
        try {
          unlockBlock(lockId);
        } catch (BlockDoesNotExistException e1) {
          errors.add(e1);
        }
      }
      handleErrors(errors);
    }
    return blockIdToLockId;
  }

  private void handleErrors(List<Throwable> errors) throws IOException {
    if (!errors.isEmpty()) {
      StringBuilder errorStr = new StringBuilder();
      errorStr.append("failed to lock all blocks of worker ").append("host").append("\n");
      for (Throwable error : errors) {
        errorStr.append(error).append("\n");
      }
      throw new IOException(errorStr.toString());
    }
  }

  private void writeRemote(long blockId, long blockSize, byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
            PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    try {
      int tLen = len;
      int tOff = off;
      while (tLen > 0) {
        if (currentBlockOutStream == null || currentBlockOutStream.remaining() == 0) {
          getNextBlock(blockId, blockSize);
          if (currentBlockOutStream == null) {
            System.out.println("blockOutStream is null");
          } else {
            System.out.println("blockOutStream is 0");
          }
        }
        long currentBlockLeftBytes = currentBlockOutStream.remaining();
        if (currentBlockLeftBytes >= tLen) {
          currentBlockOutStream.write(b, tOff, tLen);
          tLen = 0;
        } else {
          currentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
          tLen -= currentBlockLeftBytes;
          tOff += currentBlockLeftBytes;
        }
      }
    } catch (Exception e) {
      handleCacheWriteException(e);
    }
  }


  private void getNextBlock(long blockId, long blockSize) throws IOException {
    FileSystemContext context = FileSystemContext.INSTANCE;
    alluxio.client.file.FileSystemMasterClient masterClient = context.acquireMasterClient();
    URIStatus status;
    WorkerNetAddress address;
    CreateFileOptions options = CreateFileOptions.defaults();
    OutStreamOptions outStreamOptions = options.toOutStreamOptions();
    outStreamOptions.setLocationPolicy(locationPolicy);
    status = masterClient.getStatus(pathFromBlockid(blockId), GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    context.releaseMasterClient(masterClient);
    if (currentBlockOutStream != null) {
      Preconditions.checkState(currentBlockOutStream.remaining() <= 0, PreconditionMessage.ERR_BLOCK_REMAINING);
      currentBlockOutStream.flush();
      previousBlockOutStream.add(currentBlockOutStream);
    }
      address = locationPolicy.getWorkerForNextBlock(availableWorkerInfos, blockSize);
      outStreamOptions.setUfsPath(status.getUfsPath());
      outStreamOptions.setMountId(status.getMountId());
      currentBlockOutStream = mAlluxioBlockStore.getOutStream(blockId, blockSize, address, outStreamOptions);
  }

  private AlluxioURI pathFromBlockid(long blockId) throws IOException {
    long fileId = IdUtils.fileIdFromBlockId(blockId);
    FileInfo fileInfo = getFileInfo(fileId);
    return new AlluxioURI(fileInfo.getPath());
  }

  private void handleCacheWriteException(Exception e) throws IOException {
    if (currentBlockOutStream != null) {
      currentBlockOutStream.cancel();
    }
  }

  private void transforClose() throws IOException {
    try {
      if (currentBlockOutStream != null) {
        previousBlockOutStream.add(currentBlockOutStream);
      }
      for (BlockOutStream blockOutStream : previousBlockOutStream) {
        blockOutStream.close();
      }
    } catch (Throwable e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void cleanupSession(long sessionId) {
    mBlockStore.cleanupSession(sessionId);
    mUnderFileSystemBlockStore.cleanupSession(sessionId);
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceWorkerMetricsServlet}.
   */
  @ThreadSafe
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String BLOCKS_CACHED = "BlocksCached";

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the block worker handle
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getCapacityBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getCapacityBytes() - blockWorker.getStoreMeta()
                  .getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(BLOCKS_CACHED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return blockWorker.getStoreMetaFull().getNumberOfBlocks();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
}
