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

package alluxio.master.block.options;

import alluxio.thrift.RemoveWorkerTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for removing worker Created by shawn-pc on 9/23/17.
 */
@NotThreadSafe
public final class RemoveWorkerOptions {

  private boolean mTransferCache;
  private boolean mPersist;

  /**
   * @return the default {@link RemoveWorkerOptions}
   */
  public static RemoveWorkerOptions defaults() {
    return new RemoveWorkerOptions();
  }

  /**
   * Creates a new instance of {@link RemoveWorkerOptions} from {@link RemoveWorkerTOptions}.
   *
   * @param options Thrift options
   */
  public RemoveWorkerOptions(RemoveWorkerTOptions options) {
    this();
    if (options != null) {
      if (options.isSetTransferCache()) {
        mTransferCache = options.isTransferCache();
      }
      if (options.isSetPersist()) {
        mPersist = options.isPersist();
      }
    }
  }

  private RemoveWorkerOptions() {
    mTransferCache = true;
    mPersist = false;
  }

  /**
   *
   * @return the transfer cache flag; if true, the worker will transfer the cache data to other
   *         workers before removed.
   */
  public boolean isTransferCache() {
    return mTransferCache;
  }

  /**
   * @param transferCache the transfer cache flag to use; if true, the worker will transfer the
   *        cache data to other workers before removed.
   *
   * @return the updated options object
   */
  public RemoveWorkerOptions setTransferCache(boolean transferCache) {
    mTransferCache = transferCache;
    return this;
  }

  /**
   *
   * @return the persist flag; if true, the worker will persist the non-persisted data before
   *         removed.
   */
  public boolean isPersist() {
    return mPersist;
  }

  /**
   * @param persist the persist flag to use; if true, the worker will persist the non-persisted data
   *        before removed.
   *
   * @return the updated options object
   */
  public RemoveWorkerOptions setPersist(boolean persist) {
    mPersist = persist;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RemoveWorkerOptions)) {
      return false;
    }
    RemoveWorkerOptions that = (RemoveWorkerOptions) o;
    return Objects.equal(mTransferCache, that.mTransferCache)
        && Objects.equal(mPersist, that.mPersist);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTransferCache, mPersist);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("transfer cache", mTransferCache)
        .add("persist", mPersist).toString();
  }
}
