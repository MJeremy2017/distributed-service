package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private final ZooKeeper zooKeeper;
    private static final String SERVICE_REGISTRY_ROOT = "/service_registry";
    private List<String> serviceAddresses = null;
    private String currentNodePath = null;
    private final int port;


    public ServiceRegistry(ZooKeeper zooKeeper, int port) throws InterruptedException, KeeperException, UnknownHostException {
        this.zooKeeper = zooKeeper;
        this.port = port;
        createRegistryPath();
    }

    public void createRegistryPath() throws InterruptedException, KeeperException {
        Stat stat = this.zooKeeper.exists(SERVICE_REGISTRY_ROOT, false);
        if (stat == null) {
            String path = zooKeeper.create(SERVICE_REGISTRY_ROOT, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Created service registry path:" + path);
        }
    }

    public void registerToCluster() throws InterruptedException, KeeperException, UnknownHostException {
        String workerPath = SERVICE_REGISTRY_ROOT + "/worker_";
        if (currentNodePath != null && zooKeeper.exists(currentNodePath, false) != null)
            return;
        String workerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);

        String path = zooKeeper.create(workerPath,
                workerAddress.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        currentNodePath = path;
        System.out.println("Registered to cluster with path:" + path);
    }

    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        Stat nodeStat = currentNodePath != null ? zooKeeper.exists(currentNodePath, false) : null;
        if (nodeStat == null)
            return;
        System.out.println(currentNodePath + " de-registered from cluster");
        zooKeeper.delete(currentNodePath, -1);
    }

    public synchronized List<String> getAllAddresses() throws InterruptedException, KeeperException {
        if (serviceAddresses == null)
            updateAddresses();
        return serviceAddresses;
    }

    public void registerForUpdates() throws InterruptedException, KeeperException {
        updateAddresses();
    }

    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        // set watch on the parent node
        List<String> children = this.zooKeeper.getChildren(SERVICE_REGISTRY_ROOT, this);
        List<String> addresses = new ArrayList<>();
        for (String child : children) {
            String childPath = SERVICE_REGISTRY_ROOT + "/" + child;
            Stat childStat = this.zooKeeper.exists(childPath, false);
            if (childStat == null)
                continue;
            byte[] data = this.zooKeeper.getData(childPath, this, childStat);
            addresses.add(new String(data));
        }
        serviceAddresses = addresses;
        System.out.println("Cluster addresses are:" + serviceAddresses);
    }

    @Override
    public void process(WatchedEvent event) {
        // Upon any changes, update service addresses
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

    }

}
