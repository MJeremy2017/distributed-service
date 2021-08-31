import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher {
    private static final String HOST = "localhost:2181";
    private static final int TIMEOUT = 3000;
    private static final String WATCH_NODE = "/some_znode";
    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException {
        WatcherDemo watch = new WatcherDemo();
        watch.connect();
        watch.run();
        watch.close();
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void connect() throws IOException {
        zooKeeper = new ZooKeeper(HOST, TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None -> {
                switch (event.getState()) {
                    case SyncConnected -> System.out.println("Client connected");
                    default -> {
                        synchronized (zooKeeper) {
                            zooKeeper.notifyAll();
                        }
                        System.out.println("Client disconnected");
                    }
                }
            }
            case NodeCreated -> System.out.println(WATCH_NODE + " created");
            case NodeDataChanged -> System.out.println(WATCH_NODE + " data changed");
            case NodeDeleted -> System.out.println(WATCH_NODE + " deleted");
            case NodeChildrenChanged -> System.out.println(WATCH_NODE + " children changed");
            default -> System.out.println(WATCH_NODE + " other changes");
        }

        try {
            registerWatcher();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    private void registerWatcher() throws InterruptedException, KeeperException {
        // monitor the node
        Stat stat = zooKeeper.exists(WATCH_NODE, this);
        if (stat == null) {
            return;
        }
        // if node exists
        // monitor node data
        byte[] data = zooKeeper.getData(WATCH_NODE, this, stat);
        // monitor node children
        List<String> children = zooKeeper.getChildren(WATCH_NODE, this);
        System.out.println("Get data" + new String(data) + " | children:" + children);
    }

}
