package alluxio.shell.command;

import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shawn-pc on 10/17/17.
 * Delete workers from the cluster by the hosts in args and transfer partial blocks from the deleted workers to
 * the rest workers.
 */
@ThreadSafe
public class DeleteWorkerCommand extends AbstractShellCommand{
    public DeleteWorkerCommand(FileSystem fs) {
        super(fs);
    }

    @Override
    public String getCommandName() {
        return "deleteworker";
    }

    @Override
    public int run(CommandLine cl) throws AlluxioException, IOException {
        String[] args = cl.getArgs();
        List<String> hosts = new ArrayList<>();
        for (String host : args) {
            hosts.add(host);
        }
        try(CloseableResource<BlockMasterClient> client = FileSystemContext.INSTANCE.acquireBlockMasterClientResource()) {
            client.get().deleteWorker(hosts);
        }
        return 0;
    }

    @Override
    public String getUsage() {
        return "deleteworker <host1> [host2] ... [hostn]";
    }

    @Override
    public String getDescription() {
        return "Delete workers from the cluster by the hosts in args and " +
                "transfer partial blocks from the deleted workers to the rest workers.";
    }

    @Override
    protected int getNumOfArgs() {
        return 1;
    }

    @Override
    public boolean validateArgs(String... args) {
        boolean valid = args.length >= getNumOfArgs();
        if (!valid) {
            System.out.println(getCommandName() + " takes " + getNumOfArgs() + " argument at least\n");
        }
        return valid;
    }
}
