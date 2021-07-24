import cluster.management.LeaderElection;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int port = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application app = new Application();
        ZooKeeper zooKeeper = app.connect();  // the zookeeper instance that owns multiple watchers
        ServiceRegistry registry = new ServiceRegistry(zooKeeper, port);
        LeaderElection le = new LeaderElection(zooKeeper, registry);
        le.volunteerForLeadership();
        le.electLeader();

        app.run();
        app.close();
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    public ZooKeeper connect() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected -> System.out.println("Client connected");
                case Expired -> System.out.println("Client connection expired");
                default -> {
                    System.out.println("Client disconnected");
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                }
            }

        }
    }
}
