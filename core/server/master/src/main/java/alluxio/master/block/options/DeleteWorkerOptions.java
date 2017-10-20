package alluxio.master.block.options;

import alluxio.thrift.DeleteWorkerTOptions;
import alluxio.thrift.RemoveWorkerTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for delete worker Created by shawn-pc on 10/12/17.
 */
@NotThreadSafe
public class DeleteWorkerOptions {
    private long mWorkerNumber;

    /**
     * @return the default {@link RemoveWorkerOptions}
     */
    public static DeleteWorkerOptions defaults() {
        return new DeleteWorkerOptions();
    }

    /**
     * Creates a new instance of {@link RemoveWorkerOptions} from {@link RemoveWorkerTOptions}.
     *
     * @param options Thrift options
     */
    public DeleteWorkerOptions(DeleteWorkerTOptions options) {
        this();
    }

    private DeleteWorkerOptions() {
        mWorkerNumber = 0;
    }

    public long getWorkerNumber() {
        return mWorkerNumber;
    }

    public DeleteWorkerOptions setWorkerNumber(long WorkerNumber) {
        mWorkerNumber = mWorkerNumber;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeleteWorkerOptions)) {
            return false;
        }
        DeleteWorkerOptions that = (DeleteWorkerOptions) o;
        return Objects.equal(mWorkerNumber, that.mWorkerNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mWorkerNumber);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("Worker number", mWorkerNumber).toString();
    }
}
