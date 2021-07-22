import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ROOT_ELECTION_PATH = "/election";
    private String CurrentZnodeName;
    private ZooKeeper zooKeeper;  // zookeeper client

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // main thread
        LeaderElection le = new LeaderElection();
        le.connectToZookeeperServer();
        le.volunteerForLeadership();
        le.electLeader();
        // give the lock to the current thread and put the current thread to waiting status
        le.waitConnect();
        le.close();
        System.out.println("Disconnected from ZooKeeper");
    }

    private void electLeader() throws InterruptedException, KeeperException {
        Stat previousNodeStat = null;
        System.out.println("Current node name is:" + CurrentZnodeName);
        while (previousNodeStat == null) {
            List<String> childNames = zooKeeper.getChildren(ROOT_ELECTION_PATH, null);
            Collections.sort(childNames);
            String smallestChild = childNames.get(0);
            if (CurrentZnodeName.equals(smallestChild)) {
                System.out.println("This is the leader");
                return;
            }

            // if not leader, watch previous node
            System.out.println("I am not the leader, leader node is:" + smallestChild);
            int indexOfPreviousNode = childNames.indexOf(CurrentZnodeName) - 1;
            String previousZnodePath = ROOT_ELECTION_PATH + "/" + childNames.get(indexOfPreviousNode);
            System.out.println("Set watch on previous znode path: " + previousZnodePath);
            previousNodeStat = zooKeeper.exists(previousZnodePath, this);
        }
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String PATH = ROOT_ELECTION_PATH + "/_c";
        String znodePath = zooKeeper.create(PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created znode full path:" + znodePath);
        CurrentZnodeName = znodePath.replace(ROOT_ELECTION_PATH + "/", "");
    }


    public void waitConnect() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void connectToZookeeperServer() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent event) {
        // handled on separate zookeeper event thread
        switch (event.getType()) {
            case None -> {
                switch (event.getState()) {
                    case SyncConnected -> System.out.println("Client connected to Zookeeper server");
                    case Expired -> System.out.println("Connection expired");
                    default -> {
                        synchronized (zooKeeper) {
                            System.out.println("Client disconnected to ZooKeeper event");
                            zooKeeper.notifyAll();
                        }
                    }
                }
            }
            case NodeDeleted -> {
                try {
                    electLeader();
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}
