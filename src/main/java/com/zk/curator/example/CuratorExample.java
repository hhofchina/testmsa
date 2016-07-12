package com.zk.curator.example;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorTempFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.locks.RevocationListener;
import org.apache.curator.framework.recipes.locks.Revoker;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.annotate.JsonRootName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.collect.Lists;

import discovery.InstanceDetails;

public class CuratorExample implements java.lang.AutoCloseable {
	static Logger logger = LoggerFactory.getLogger(CuratorExample.class);
	private CuratorFramework client;
	// 默认zookeeper 服务器连接地址
	private String constring = "localhost:2181";// 多个地址逗号分割:"localhost:2181,localhost:2282"
	private TestingServer testingserver;
	private PathChildrenCache cache;

	public CuratorExample() {
		// ExponentialBackoffRetry
		// 指数后退型重试策略：重试第一次等待1秒，第2次等待2秒，第3次等待4秒，第4次等待8秒。。。
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 4);
		// 其他重试策略
		// new RetryNTimes(5, 1000);//重试5次，间隔1000ms
		// new RetryOneTime(1000);//重试1次，间隔1000ms
		// new RetryUntilElapsed(60000,1000);//重试间隔1000ms，直到重试用时60000ms
		// 自定义重试策略：实现接口RetryPolicy：重试前，调用allowRetry,传入当前重试次数和操作逝去时间
		// public boolean allowRetry(int retryCount, long elapsedTimeMs)

		// 用模拟测试服务器代替zkshever,需要手动关闭
		// try {
		// TestingServer testingserver = new TestingServer();
		// constring = testingserver.getConnectString();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		System.out.println(">>>Connect to server:" + constring);
		// 简单策略
		client = CuratorFrameworkFactory.newClient(constring, retryPolicy);
		// //更多选项
		// client = CuratorFrameworkFactory.builder()
		// .namespace("example") //指定根namespace，以后所有的路径默认就是/example/..
		// .connectString(constring)
		// .retryPolicy(retryPolicy)
		// .connectionTimeoutMs(5000)
		// .sessionTimeoutMs(60000)
		// .build();

	}

	// client 启动后，可选启动缓存
	public void initCache(String path) {
		cache = new PathChildrenCache(client, path, true);
	}

	public void close() {
		if (client != null) {
			// client.close();
			CloseableUtils.closeQuietly(client);
		}
		if (testingserver != null)
			CloseableUtils.closeQuietly(testingserver);
		// 还可以关闭cache
		if (cache != null)
			CloseableUtils.closeQuietly(cache);
	}

	public void delete(String path) throws Exception {
		if (null != client.checkExists().forPath(path))
			client.delete().inBackground().forPath(path);
	}

	public void create(String path) throws Exception {
		if (null == client.checkExists().forPath(path))
			client.create().forPath(path);
	}

	public void createWithParent(String path, byte[] data) throws Exception {
		if (null == client.checkExists().forPath(path))
			client.create().creatingParentsIfNeeded().forPath(path, data);
	}

	public void createTmpNode() throws Exception {
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.forPath("/test/tmp", new byte[0]);
	}

	public void testReadData(String path) throws Exception {
		Object data = client.getData().forPath(path);
		if (null != data) {
			System.out.println("Data is :" + data);
		}
	}

	public void testLock() throws Exception {
		InterProcessMutex lock = new InterProcessMutex(client, "/locksome");
		if (lock.acquire(60L, TimeUnit.SECONDS)) {
			try {
				System.out.println("acquired a lock");
			} finally {
				lock.release();
			}
		} else {
			System.out.println("can not acquired a lock");
		}
	}

	public static class LeaderClient extends LeaderSelectorListenerAdapter
			implements Closeable {
		// 计数器，记录当前实例成为leader的次数
		private AtomicInteger leaderCount = new AtomicInteger();
		private String name;
		private final LeaderSelector leaderSelector;

		public LeaderClient(CuratorFramework client, String path, String name) {
			this.name = name;
			leaderSelector = new LeaderSelector(client, path, this);
			leaderSelector.autoRequeue();// 确保此实例以后释放领导权后还可以获取领导权
		}

		public void start() {
			leaderSelector.start();
		}

		public void close() {
			CloseableUtils.closeQuietly(leaderSelector);
		}

		// 执行leader工作，分配任务等，如果此方法如不返回，死循环，这此实例一直是leader
		public void takeLeadership(CuratorFramework client) throws Exception {
			System.out.println("I'am leader now");

			final int waitSeconds = (int) (5 * Math.random()) + 1;
			System.out.println(name + " is now the leader. Waiting "
					+ waitSeconds + " seconds...");
			System.out.println(name + " has been leader "
					+ leaderCount.getAndIncrement() + " time(s) before.");
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
			} catch (InterruptedException e) {
				System.err.println(name + " was interrupted.");
				Thread.currentThread().interrupt();
			} finally {
				System.out.println(name + " give up leadership.\n");
			}
		}
	}

	public void testLeader() throws Exception {
		final int CLIENT_QTY = 10;
		final String PATH = "/examples/leader";

		List<CuratorFramework> clients = Lists.newArrayList();
		List<LeaderClient> leaders = Lists.newArrayList();
		// 模拟测试服务器
		TestingServer server = new TestingServer();
		try {
			for (int i = 0; i < CLIENT_QTY; ++i) {
				CuratorFramework client = CuratorFrameworkFactory.newClient(
						server.getConnectString(), new ExponentialBackoffRetry(
								1000, 3));
				clients.add(client);
				LeaderClient leadclient = new LeaderClient(client, PATH,
						"Client #" + i);
				leaders.add(leadclient);
				client.start();
				leadclient.start();
			}

			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			System.out.println("Shutting down...");
			for (LeaderClient c : leaders) {
				CloseableUtils.closeQuietly(c);
			}
			for (CuratorFramework c : clients) {
				CloseableUtils.closeQuietly(c);
			}
		}
	}

	// 单线程单个计数器
	public void counter_ShareCount() throws Exception {
		// 计数器
		try (SharedCount counter = new SharedCount(client, "/testcount", 111)) {
			counter.start();
			counter.addListener(new SharedCountListener() {
				@Override
				public void stateChanged(CuratorFramework cf, ConnectionState v) {
					System.out.println(" state changed");
				}

				@Override
				public void countHasChanged(SharedCountReader scr, int v)
						throws Exception {
					System.out.println(scr.getCount() + "\t count changed:" + v);
				}
			});

			System.out.println(">>>\n counter is:" + counter.getCount());
			if (counter.trySetCount(counter.getVersionedValue(), 222)) {
				System.out.println("try set ok");
			} else
				System.out.println("try set failed");

			System.out.println(">>>\n counter is:" + counter.getCount());
			counter.setCount(333);
			System.out.println(">>>\n counter is:" + counter.getCount());
		}
	}

	/**
	 * 乐观锁，分布式原子计数
	 * 
	 * @throws Exception
	 */
	public void counter_DistributedAtomicLong_Optimistic() throws Exception {
		DistributedAtomicLong d = new DistributedAtomicLong(client, "/user_id",
				new ExponentialBackoffRetry(1000, 3));
		d.increment();
		AtomicValue<Long> v = d.get();
		if (v.succeeded()) {
			System.out.println(">>>\n get ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n get failed: pre:" + v.preValue() + "=>"
					+ v.postValue());

		v = d.add(100L);
		if (v.succeeded()) {
			System.out.println(">>>\n add ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n add failed: pre:" + v.preValue() + "=>"
					+ v.postValue());

		v = d.increment();
		if (v.succeeded()) {
			System.out.println(">>>\n increment ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n increment failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
		v = d.decrement();
		if (v.succeeded())
			System.out.println(">>>\n decrement ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		else
			System.out.println(">>>\n decrement failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
		v = d.subtract(50L);
		if (v.succeeded())
			System.out.println(">>>\n subtract ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		else
			System.out.println(">>>\n subtract failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
	}

	/**
	 * 互斥锁，原子计数
	 * 
	 * @throws Exception
	 */
	public void counter_DistributedAtomicLong_Mutex() throws Exception {
		DistributedAtomicLong d = new DistributedAtomicLong(client,
				"/user_mutex_id", new ExponentialBackoffRetry(1000, 3),
				PromotedToLock.builder().lockPath("/lock_user_mutex_id")
						.build()// 要指定一个锁定目录，以实现互斥锁，关闭锁后自动删除
		);
		d.increment();
		AtomicValue<Long> v = d.get();
		if (v.succeeded()) {
			System.out.println(">>>\n get ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n get failed: pre:" + v.preValue() + "=>"
					+ v.postValue());

		v = d.add(100L);
		if (v.succeeded()) {
			System.out.println(">>>\n add ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n add failed: pre:" + v.preValue() + "=>"
					+ v.postValue());

		v = d.increment();
		if (v.succeeded()) {
			System.out.println(">>>\n increment ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		} else
			System.out.println(">>>\n increment failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
		v = d.decrement();
		if (v.succeeded())
			System.out.println(">>>\n decrement ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		else
			System.out.println(">>>\n decrement failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
		v = d.subtract(50L);
		if (v.succeeded())
			System.out.println(">>>\n subtract ok: pre:" + v.preValue() + "=>"
					+ v.postValue());
		else
			System.out.println(">>>\n subtract failed: pre:" + v.preValue()
					+ "=>" + v.postValue());
	}

	public static void testLeaderLatch() throws Exception {
		List<CuratorFramework> clientList = Lists.newArrayList();
		List<LeaderLatch> latchList = Lists.newArrayList();
		TestingServer server = new TestingServer();
		final int CLIENT_QTY = 10;
		final String PATH = "/examples/leader";
		try {
			for (int i = 0; i < CLIENT_QTY; i++) {
				CuratorFramework client = CuratorFrameworkFactory.newClient(
						server.getConnectString(), new ExponentialBackoffRetry(
								1000, 3));
				clientList.add(client);
				LeaderLatch latch = new LeaderLatch(client, PATH, "Client#" + i);
				latchList.add(latch);
				client.start();
				latch.start();
			}
			Thread.sleep(20000);
			LeaderLatch currentLeader = null;
			for (int i = 0; i < CLIENT_QTY; i++) {
				LeaderLatch latch = latchList.get(i);
				if (latch.hasLeadership())
					currentLeader = latch;
			}
			System.out.println("current leader is " + currentLeader.getId());
			System.out.println("release the leader " + currentLeader.getId());
			currentLeader.close();
			latchList.get(0).await(2, TimeUnit.SECONDS);
			System.out
					.println("Client #0 maybe is elected as the leader or not although it want to be");
			System.out.println("the new leader is "
					+ latchList.get(0).getLeader().getId());

			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			System.out.println("Shutting down...");
			for (LeaderLatch exampleClient : latchList) {
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clientList) {
				CloseableUtils.closeQuietly(client);
			}
			CloseableUtils.closeQuietly(server);
		}
	}

	// 模拟受限资源，限制单线程访问。
	public static class FakeLimitResource {
		private final AtomicBoolean inUse = new AtomicBoolean(false);

		public void use() throws InterruptedException {
			if (!inUse.compareAndSet(false, true)) {
				throw new IllegalStateException("同一时刻，只能一个线程访问此资源");
			}
			try {
				System.out.println("\t**受限资源访问中");
				Thread.sleep((long) (5000 * Math.random()));
			} finally {
				inUse.set(false);
			}
		}
	}

	// 测试可重入锁，访问受限资源
	public static void testSharedLock() throws Exception {
		// TestingServer server = new TestingServer();
		final String PATH = "/test_sharedrentrantlock";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		try {
			ExecutorService service = Executors.newFixedThreadPool(10);// 线程池
			FakeLimitResource resource = new FakeLimitResource();
			for (int i = 0; i < 10; i++) {
				final int ID = i;
				Callable<Void> task = new Callable<Void>() {
					public Void call() throws Exception {
						try (CuratorFramework client = CuratorFrameworkFactory
								.newClient("localhost:2181"/*
															 * server.
															 * getConnectString
															 * ()
															 */, retryPolicy)) {

							// 监听连接状态变化
							client.getConnectionStateListenable().addListener(
									new ConnectionStateListener() {
										@Override
										public void stateChanged(
												CuratorFramework curator,
												ConnectionState newState) {
											System.out.println(ID
													+ " ** STATE CHANGED TO : "
													+ newState);
											if (newState
													.equals(ConnectionState.SUSPENDED)) {
												System.out
														.println(ID
																+ " ** is SUSPENDED : ");
											}
										}
									});

							client.start();

							// 可重入锁，一个线程里允许acquire多次，需相应多次release
							// InterProcessMutex lock = new InterProcessMutex(
							// client, PATH);
							// 不可重入锁（信号量），一个线程多次acquire会被阻塞导致超时
							// InterProcessSemaphoreMutex lock = new
							// InterProcessSemaphoreMutex(
							// client, PATH);
							// 可重入读写锁
							InterProcessReadWriteLock rwlock = new InterProcessReadWriteLock(
									client, PATH);
							// 共享读
							// InterProcessMutex lock = rwlock.readLock();
							// 排斥锁
							InterProcessMutex lock = rwlock.writeLock();

							lock.makeRevocable(new RevocationListener<InterProcessMutex>() {
								@Override
								public void revocationRequested(
										InterProcessMutex l) {
									System.out.println(ID + " \t 被要求释放锁");
								}
							});
							// 要求锁占有者释放锁。
							Revoker.attemptRevoke(client, PATH);

							if (!lock.acquire(10, TimeUnit.SECONDS)) {
								throw new IllegalStateException(ID + " 无法获得锁");
							}
							lock.acquire();
							try {
								System.out.println(ID + " 获得锁");
								resource.use();
							} finally {
								System.out.println(ID + " 释放锁");
								lock.release();
								lock.release();
							}
						}
						return null;
					}
				};
				// 提交任务到线程池
				service.submit(task);
			}
			// 关闭任务
			service.shutdown();
			service.awaitTermination(10, TimeUnit.SECONDS);

		} finally {
			// CloseableUtils.closeQuietly(server);
		}
	}

	// 测试信号量，访问受限资源
	public static void testSharedSemaphore() throws Exception {
		final String PATH = "/test_SharedSemaphore";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final int MAX_LEASE = 10;
		ExecutorService service = Executors.newFixedThreadPool(10);// 线程池
		FakeLimitResource resource = new FakeLimitResource();
		try (CuratorFramework client = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			client.start();
			// 最多10个租约
			InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(
					client, PATH, MAX_LEASE);
			Collection<Lease> leases = semaphore.acquire(5);
			System.out.println(" get " + leases.size() + " leases");
			Lease lease = semaphore.acquire();
			System.out.println("get another lease");
			try {
				resource.use();
				Collection<Lease> leases2 = semaphore.acquire(8, 10,
						TimeUnit.SECONDS);
				System.out.println("应该超时，无足够的Lease可用 " + leases2);
			} finally {
				// 释放租约
				semaphore.returnLease(lease);
				semaphore.returnAll(leases);
			}
		}
	}

	public static void testBarrier() throws Exception {
		final int CNT = 10;
		final String PATH = "/test_barrier";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		List<CuratorFramework> clients = Lists.newArrayList();
		ExecutorService service = Executors.newFixedThreadPool(CNT);// 线程池
		try (CuratorFramework client = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			client.start();
			DistributedBarrier d = new DistributedBarrier(client, PATH);
			d.setBarrier();

			for (int i = 0; i < CNT; i++) {
				final int ID = i;
				// CuratorFramework c = CuratorFrameworkFactory.newClient(
				// "localhost:2181", retryPolicy);
				// c.start();
				// clients.add(c);

				// final DistributedBarrier db = new DistributedBarrier(c,
				// PATH);
				Callable<Void> task = new Callable<Void>() {
					public Void call() throws Exception {
						try (CuratorFramework c = CuratorFrameworkFactory
								.newClient("localhost:2181", retryPolicy)) {
							c.start();
							DistributedBarrier db = new DistributedBarrier(c,
									PATH);

							System.err.println(ID + "\t is waiting");
							Thread.sleep((long) (2000 * Math.random()));
							db.waitOnBarrier();
							// db.waitOnBarrier(10, TimeUnit.SECONDS);
							System.err.println(ID + "\t is running");
						}
						return null;
					}
				};
				service.submit(task);
			}
			System.err.println(">>> press any key to remove barrier");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
			d.removeBarrier();
			service.shutdown();
			service.awaitTermination(20, TimeUnit.SECONDS);
		} finally {
			// for (CuratorFramework c : clients) {
			// CloseableUtils.closeQuietly(c);
			// }
		}
	}

	// 双栅栏，在计算开始和结束同步，达到指定数量才启动计算
	public static void testDoubleBarrier() throws Exception {
		final int CNT = 100;
		final String PATH = "/test_doublebarrier";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		ExecutorService service = Executors.newFixedThreadPool(CNT);// 线程池
		for (int i = 0; i < CNT - 1; i++) {
			final int ID = i;
			Callable<Void> task = new Callable<Void>() {
				public Void call() throws Exception {
					try (CuratorFramework c = CuratorFrameworkFactory
							.newClient("localhost:2181", retryPolicy)) {
						c.start();
						DistributedDoubleBarrier db = new DistributedDoubleBarrier(
								c, PATH, CNT);
						System.err.println(ID + "\t is waiting");
						db.enter();
						System.err.println(ID + "\t is enter");
						// Thread.sleep((long) (5000 * Math.random()));
						// db.leave(10, TimeUnit.SECONDS);
						db.leave();
						System.err.println(ID + "\t is leaving");
					}
					return null;
				}
			};
			service.submit(task);

		}
		System.err.println(">>> press any key to start last one");
		new BufferedReader(new InputStreamReader(System.in)).readLine();
		try (CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();
			DistributedDoubleBarrier db = new DistributedDoubleBarrier(c, PATH,
					CNT);
			db.enter();
			System.err.println("last one entered");
			db.leave();
		}

		service.shutdown();
		service.awaitTermination(20, TimeUnit.SECONDS);
	}

	// 多线程多连接多计数器，同一路径
	public static void testCounterShareCounter() throws Exception {
		final int CNT = 10;
		final String PATH = "/test_counter_shared";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		ExecutorService service = Executors.newFixedThreadPool(CNT);// 线程池
		final Random rand = new Random();
		for (int i = 0; i < CNT; i++) {
			final int ID = i;
			Callable<Void> task = new Callable<Void>() {
				public Void call() throws Exception {
					try (CuratorFramework c = CuratorFrameworkFactory
							.newClient("localhost:2181", retryPolicy)) {
						c.start();
						try (SharedCount count = new SharedCount(c, PATH, 0)) {// 初始值如无设置，默认0
							count.start();
							Thread.sleep((long) rand.nextInt(5000));
							int pre = count.getVersionedValue().getValue();
							if (count.trySetCount(count.getVersionedValue(),
									count.getCount() + rand.nextInt(100))) {
								System.out.println(ID + "\t" + pre + "=>"
										+ count.getCount());
							} else {
								System.out
										.println(ID + "\t trySetCount Failed");
							}
						}
					}
					return null;
				}
			};
			service.submit(task);
		}
		System.err.println(">>> press any key to quit");
		new BufferedReader(new InputStreamReader(System.in)).readLine();
		service.shutdown();
		service.awaitTermination(5, TimeUnit.SECONDS);
	}

	// 多线程多连接多原子计数器，同一路径
	public static void testDistributedAtomLong() throws Exception {
		final int CNT = 10;
		final String PATH = "/test_counter_atom";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 4);
		ExecutorService service = Executors.newFixedThreadPool(CNT);// 线程池
		final Random rand = new Random();
		for (int i = 0; i < CNT; i++) {
			final int ID = i;
			Callable<Void> task = new Callable<Void>() {
				public Void call() throws Exception {
					try (CuratorFramework c = CuratorFrameworkFactory
							.newClient("localhost:2181", retryPolicy)) {
						c.start();
						// 双栅栏同步并发运行
						DistributedDoubleBarrier db = new DistributedDoubleBarrier(
								c, PATH, CNT + 1);
						db.enter();
						// 初始值默认0
						DistributedAtomicLong count = new DistributedAtomicLong(
								c, PATH, retryPolicy);

						// Thread.sleep((long) rand.nextInt(5000));
						AtomicValue<Long> v = count.increment();
						if (v.succeeded()) {
							System.out.println(ID + "\tincrease" + v.preValue()
									+ "=>" + v.postValue());
						} else
							System.out.println(ID + "\t increase Failed");
						v = count.decrement();
						if (v.succeeded()) {
							System.out.println(ID + "\tdecrease" + v.preValue()
									+ "=>" + v.postValue());
						} else
							System.out.println(ID + "\t decrement Failed");
						v = count.add((long) (rand.nextInt(100)));
						if (v.succeeded()) {
							System.out.println(ID + "\t add:" + v.preValue()
									+ "=>" + v.postValue());
						} else
							System.out.println(ID + "\t add Failed");
						v = count.subtract((long) (rand.nextInt(10)));
						if (v.succeeded()) {
							System.out.println(ID + "\t substract:"
									+ v.preValue() + "=>" + v.postValue());
						} else
							System.out.println(ID + "\t substract Failed");

						db.leave(10, TimeUnit.SECONDS);
					}
					return null;
				}
			};
			service.submit(task);
		}
		System.err.println(">>> press any key to quit");
		new BufferedReader(new InputStreamReader(System.in)).readLine();

		try (CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();
			DistributedDoubleBarrier db = new DistributedDoubleBarrier(c, PATH,
					CNT + 1);
			db.enter();
			System.err.println("last one entered");
			db.leave();
		}

		service.shutdown();
		service.awaitTermination(5, TimeUnit.SECONDS);
	}

	public static void testPathCache() throws Exception {
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final String PATH = "/test_pathcache";
		try (CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();
			try (PathChildrenCache cache = new PathChildrenCache(c, PATH, true)) {
				// 通过client设置数据
				if (null == c.checkExists().forPath(PATH))
					c.create().creatingParentsIfNeeded().forPath(PATH);
				c.setData().forPath(PATH, "root".getBytes());
				if (null == c.checkExists().forPath(PATH + "/temp"))
					c.create().creatingParentsIfNeeded()
							.forPath(PATH + "/temp", "first_temp".getBytes());

				// cache.start();
				// cache.start(StartMode.NORMAL);
				// cache.start(StartMode.BUILD_INITIAL_CACHE);
				cache.start(StartMode.POST_INITIALIZED_EVENT);
				for (ChildData cd : cache.getCurrentData()) {
					System.out.println(cd.getPath() + " = "
							+ new String(cd.getData()));
				}
				cache.getListenable().addListener(
						new PathChildrenCacheListener() {
							@Override
							public void childEvent(CuratorFramework cc,
									PathChildrenCacheEvent ee) throws Exception {
								System.out.println("event:"
										+ ee.getData().getPath() + " = "
										+ new String(ee.getData().getData()));
							}
						});
				c.delete().forPath(PATH + "/temp");
				System.err.println(">>> press any key to quit");
				new BufferedReader(new InputStreamReader(System.in)).readLine();
			}
		}
	}

	/**
	 * 测试临时持久节点，连接会话/断开时，节点自动删除。这个特性可用于服务注册。
	 * 
	 * @throws Exception
	 */
	public static void testPersistentEphemeralNode() throws Exception {
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final String PATH = "/test_PersistentEphemeralNode";
		try (CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			// 监听连接状态变化
			c.getConnectionStateListenable().addListener(
					new ConnectionStateListener() {
						@Override
						public void stateChanged(CuratorFramework arg0,
								ConnectionState newState) {
							System.out.println("client state:"
									+ newState.name());
						}
					});
			c.start();
			// 临时持久节点，可用于服务注册，服务连接会话关闭，服务自动不可用。
			try (PersistentNode node = new PersistentNode(c,
					CreateMode.EPHEMERAL, false, PATH, "first".getBytes())) {
				node.start();
				// 等待Node初始化
				node.waitForInitialCreate(5, TimeUnit.SECONDS);
				String actpath = node.getActualPath();
				System.out.println("node " + actpath + " value: "
						+ new String(c.getData().forPath(actpath)));
				// 强制关闭会话
				KillSession.kill(c.getZookeeperClient().getZooKeeper(),
						"localhost:2181");
				System.out.println("connection closed ,and node " + actpath
						+ " doesn't exist: "
						+ (c.checkExists().forPath(actpath) == null));
			}
		}
	}

	/**
	 * 测试组成员管理
	 * 
	 * @throws Exception
	 */
	public static void testGroupMember() throws Exception {
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final String PATH = "/test_groupmembership";
		try (final CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();

			try (GroupMember gm = new GroupMember(c, PATH, "testgroupmember")) {

				Watcher w = new Watcher() {
					@Override
					public void process(WatchedEvent e) {
						System.out.println(e.getPath() + "\t" + e.getState());
					}
				};
				gm.start();
				Thread.sleep(5000L);
				// 确保group节点存在
				new EnsurePath(PATH).ensure(c.getZookeeperClient());
				// 为子节点添加观察者
				List<String> children = c.getChildren().usingWatcher(w)
						.forPath(PATH);
				System.err.println("Group members: " + children);
				System.out.println("idFromPath:"
						+ gm.idFromPath("/test_groupmembership"));
				// 添加临时节点
				String name = c.create()
						.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
						.forPath(PATH + "/tmp");

				for (Entry<String, byte[]> e : gm.getCurrentMembers()
						.entrySet()) {
					System.out.println(e.getKey() + "="
							+ new String(e.getValue()));
				}
			}
		}
	}

	/**
	 * 分布式队列。不建议使用ZK的queue，性能问题和数据大小限制
	 * 
	 * @throws Exception
	 */
	public static void testDistributeQueue() throws Exception {
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final String PATH = "/test_distributequeue";
		final int CNT = 1;
		ExecutorService service = Executors.newFixedThreadPool(CNT);// 线程池

		// 队列消费者
		final QueueConsumer<String> consumer = new Consumer<String>();
		// 队列对象序列化解析
		final QueueSerializer<String> serializer = new Serializer();
		// 清理目录
		try (final CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();
			if (null != c.checkExists().forPath(PATH))
				c.delete().inBackground().forPath(PATH);

			if (null != c.checkExists().forPath(PATH + "_db"))
				c.delete().inBackground().forPath(PATH + "_db");

		}

		// 并发多线程队列访问，zk会有性能瓶颈
		for (int i = 0; i < CNT; i++) {
			final int ID = i;
			Callable<Void> task = new Callable<Void>() {
				public Void call() throws Exception {

					try (final CuratorFramework c = CuratorFrameworkFactory
							.newClient("localhost:2181", retryPolicy)) {
						c.start();

						// 双栅栏，线程等待最后一个成员
						DistributedDoubleBarrier db = new DistributedDoubleBarrier(
								c, PATH + "_db", CNT + 1);
						db.enter();
						System.err.println(ID + "\truning");
						QueueBuilder<String> b = QueueBuilder.builder(c,
								consumer, serializer, PATH).lockPath(
								PATH + "_lock");
						try (DistributedQueue<String> queue = b.buildQueue()) {
							// DistributedQueue<String> queue =
							// b.buildDelayQueue();
							// DistributedIdQueue<String> queue =
							// b.buildIdQueue();
							// DistributedPriorityQueue<String> queue =
							// b.buildPriorityQueue(12);
							queue.start();
							for (int p = 0; p < 3; p++) {
								queue.put(ID + "_data_" + p);
							}
						}
						db.leave(600, TimeUnit.SECONDS);
					} catch (Exception ee) {
					}
					return null;
				}
			};
			try {
				service.submit(task);
			} catch (Exception ee) {
				ee.printStackTrace();
			}
		}

		System.err.println(">>> press any key to start");
		new BufferedReader(new InputStreamReader(System.in)).readLine();
		// 双栅栏，最后一个成员进入并引发并发
		try (CuratorFramework c = CuratorFrameworkFactory.newClient(
				"localhost:2181", retryPolicy)) {
			c.start();
			DistributedDoubleBarrier db = new DistributedDoubleBarrier(c, PATH
					+ "_db", CNT + 1);
			db.enter();
			System.err.println("last one entered");
			db.leave(60, TimeUnit.SECONDS);
		}
		Thread.sleep(5000L);
		service.shutdown();
		service.awaitTermination(60, TimeUnit.SECONDS);
	}

	public static class Consumer<T> implements QueueConsumer<T> {
		@Override
		public void stateChanged(CuratorFramework arg0, ConnectionState newState) {
			System.err.println("connection new state: " + newState.name());
		}

		@Override
		public void consumeMessage(T msg) throws Exception {
			System.err.println("consume one message: " + msg);
		}
	}

	public static class Serializer implements QueueSerializer<String> {
		@Override
		public String deserialize(byte[] b) {
			return new String(b);
		}

		@Override
		public byte[] serialize(String s) {
			return s.getBytes();
		}
	}

	/**
	 * 分布式队列。不建议使用ZK的queue，性能问题和数据大小限制
	 * 
	 * @throws Exception
	 */
	public static void testDistributeIDQueue() throws Exception {
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 4);
		final String PATH = "/test_distributeidqueue";
		// 队列消费者
		final QueueConsumer<String> consumer = new Consumer<String>();
		// 队列对象序列化解析
		final QueueSerializer<String> serializer = new Serializer();

		try (final CuratorFramework c = CuratorFrameworkFactory.builder()
				.connectString("localhost:2181").retryPolicy(retryPolicy)
				.connectionTimeoutMs(5000).sessionTimeoutMs(60000).build()) {
			c.start();
			c.blockUntilConnected();

			QueueBuilder<String> b = QueueBuilder.builder(c, consumer,
					serializer, PATH).lockPath(PATH + "_lock");
			try (DistributedIdQueue<String> queue = b.buildIdQueue()) {
				// DistributedIdQueue<String> queue =
				// b.buildIdQueue();
				// DistributedPriorityQueue<String> queue =
				// b.buildPriorityQueue(12);
				queue.start();
				queue.put("data_0", "data_0_id");
				// for (int p = 0; p < 10; p++) {
				// queue.put("data_" + p, "id_" + p);
				// }
				// queue.remove("id_5");
			}
		}

		System.err.println(">>> press any key to quit");
		new BufferedReader(new InputStreamReader(System.in)).readLine();
	}

	/**
	 * 指定根namespace为example，以后所有的路径默认就是/example/..
	 * 
	 * @throws Exception
	 */
	public static void testNamespaceClient() throws Exception {
		try (CuratorFramework c = CuratorFrameworkFactory.builder()
				.namespace("example").connectString("localhost:2181")
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(5000).sessionTimeoutMs(60000).build()) {
			c.start();
			if (null == c.checkExists().forPath("/first"))
				c.create().creatingParentsIfNeeded().forPath("/first");
			c.setData().forPath("/first", "first".getBytes());
			if (null != c.checkExists().forPath("/first")) {
				System.err.println("found:"
						+ new String(c.getData().forPath("/first")));
			} else
				System.err.println("not found /first");
		}
	}

	/**
	 * 临时短连接,CuratorTempFramework ，API受限功能的客户端，不活跃自动被断开。用于跨WAN的访问。
	 * 
	 * @throws Exception
	 */
	public static void testTempClient() throws Exception {
		MDC.put("name", "testTempClient");
		try (CuratorTempFramework c = CuratorFrameworkFactory
				.builder()
				// .canBeReadOnly(true) //可设置为只读连接
				.namespace("example").connectString("localhost:2181")
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(5000).sessionTimeoutMs(60000)
				.buildTemp(10, TimeUnit.SECONDS)
		// .buildTemp()
		) {

			// c.start();//临时连接无需调用start：no need start()!
			try {
				System.err.println("found:"
						+ new String(c.getData().forPath("/second")));
			} catch (NoNodeException e) {// 节点不存在直接报异常
				System.err.println("not found");
			}
		}
		MDC.put("end", "testTempClient");
		logger.info("END");
	}

	/**
	 * TestingCluster 是模拟zk cluster的测试服务器
	 * 
	 * @throws Exception
	 */
	public static void testCluster() throws Exception {
		TestingCluster testingcluster = new TestingCluster(4);// 最少3？
		testingcluster.start();// 测试集群服务器需要先启动
		System.err.println("testingcluster:"
				+ testingcluster.getConnectString());
		System.err.println(">>> press any key to start test");
		new BufferedReader(new InputStreamReader(System.in)).readLine();

		try (CuratorFramework c = CuratorFrameworkFactory.builder()
				.namespace("example")
				.connectString(testingcluster.getConnectString())
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(60000).sessionTimeoutMs(10000).build()) {
			// 监听连接变化
			c.getConnectionStateListenable().addListener(
					new ConnectionStateListener() {
						@Override
						public void stateChanged(CuratorFramework arg0,
								ConnectionState newState) {
							System.out.println(" ** STATE CHANGED TO : "
									+ newState);
							if (newState.equals(ConnectionState.SUSPENDED)) {
								System.out.println(" ** is SUSPENDED : ");
							}
						}
					});
			c.start();
			//
			// if (null != c.checkExists().forPath("/first")) {
			// System.err.println("found:"
			// + new String(c.getData().forPath("/first")));
			// } else
			// System.err.println("not found /first");
			//

		} finally {
			testingcluster.stop();
			CloseableUtils.closeQuietly(testingcluster);
		}
	}

	/**
	 * 封装服务payload信息
	 * 
	 * @author modian
	 *
	 */
	@JsonRootName("details")
	public static class InstancDetails {
		private String description;

		public InstancDetails() {
			this("");
		}

		public InstancDetails(String description) {
			this.description = description;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}
	}

	/**
	 * 服务查询，直接查询，参考https://git-wip-us.apache.org/repos/asf?p=curator.git;a=tree;
	 * f= curator-examples/src/main/java/discovery;h=60
	 * c564a91e46e913d7c5796da6c303e5970d7529;hb=HEAD
	 * 
	 * @throws Exception
	 */
	public static void testServiceDiscoveryQuery(String path, String serviceName)
			throws Exception {
		try (CuratorFramework c = CuratorFrameworkFactory.builder()
				.namespace("service").connectString("localhost:2181")
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(60000).sessionTimeoutMs(10000).build()) {
			c.start();

			try (ServiceDiscovery<InstanceDetails> sd = ServiceDiscoveryBuilder
					.builder(InstanceDetails.class)
					.client(c)
					.basePath(path)
					.serializer(
							new JsonInstanceSerializer<InstanceDetails>(
									InstanceDetails.class)).build()) {
				sd.start();
				// 查询所有服务
				System.err.println("all service is:");
				for (String name : sd.queryForNames()) {
					System.err.println(name);
					for (ServiceInstance<InstanceDetails> s : sd
							.queryForInstances(name)) {
						System.err.println(s.toString());
						// System.err.println(s.getPort());
						// System.err.println(s.getPayload().getDescription());
						// System.err.println(s.buildUriSpec());
					}
				}

				System.err.println("------query service--------");
				// 读取ServiceDiscovery服务获取最新服务实例，无需保存，每次获取最新。
				try (ServiceProvider<InstanceDetails> provider = sd
						.serviceProviderBuilder()
						.serviceName(serviceName)
						.providerStrategy(new RandomStrategy<InstanceDetails>())
						.build()) {
					provider.start();
					Thread.sleep(2500);

					ServiceInstance<InstanceDetails> s = provider.getInstance();
					if (null != s) {
						System.err.println(s.toString());
					} else {
						System.err.println("service is null");
					}
				}
			}
		}
	}

	/**
	 * 基于服务缓存的查询，缓存由观察者同步,刷新时间窗口为1-2秒 可用于偶尔查询活跃的服务。
	 * @param path
	 * @param serviceName
	 * @throws Exception
	 */
	public static void testServiceCacheQuery(String path, String serviceName)
			throws Exception {
		try (CuratorFramework c = CuratorFrameworkFactory.builder()
				.namespace("service").connectString("localhost:2181")
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(60000).sessionTimeoutMs(10000).build()) {
			c.start();

			try (ServiceDiscovery<InstanceDetails> sd = ServiceDiscoveryBuilder
					.builder(InstanceDetails.class)
					.client(c)
					.basePath(path)
					.serializer(
							new JsonInstanceSerializer<InstanceDetails>(
									InstanceDetails.class)).build()) {
				sd.start();
				// 服务缓存
				try (final ServiceCache<InstanceDetails> sc = sd
						.serviceCacheBuilder().name(serviceName).build()) {
					// 为服务缓存监听事件通知
					sc.addListener(new ServiceCacheListener() {
						@Override
						public void stateChanged(CuratorFramework c,
								ConnectionState state) {
							System.err.println("Connection change to:"
									+ state.name());
						}

						@Override
						public void cacheChanged() {
							System.err.println("cache changed");
						}
					});
					sc.start();
					// 最近用的缓存
					new Thread(new Runnable() {
						public void run() {
							for (int i = 0; i < 5; i++) {
								try {
									System.err
											.println("------query cache--------");
									for (ServiceInstance<InstanceDetails> s : sc
											.getInstances()) {
										System.err.println(s.toString());
									}
									Thread.sleep(1000);
								} catch (Exception e) {
								}// 读取当前连接的服务缓存，需要复用连接
							}
						}
					}).start();
				}
			}
		}
	}

	/**
	 * 服务自动注册/注销/更新，客户端连接断开强制注销服务
	 * 
	 * @param path
	 * @param serviceName
	 * @param port
	 * @param description
	 * @param act
	 * @throws Exception
	 */
	public static void testServiceDiscoveryRegister(String path,
			String serviceName, int port, String description, int act)
			throws Exception {
		// 自定义构造一个服务URL
		UriSpec uriSpec = new UriSpec("{scheme}://{address}:{port}");

		try (CuratorFramework c = CuratorFrameworkFactory.builder()
				.namespace("service").connectString("localhost:2181")
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.connectionTimeoutMs(60000).sessionTimeoutMs(10000).build()) {
			c.start();

			// 服务实例对象
			ServiceInstance<InstanceDetails> serviceInstance = ServiceInstance
					.<InstanceDetails> builder().name(serviceName).port(port)
					// .address("123.123.123.1")// 默认本机地址
					.payload(new InstanceDetails(description)).uriSpec(uriSpec)
					.build();

			System.err.println(serviceInstance.getPayload().getDescription());
			System.err.println(serviceInstance.buildUriSpec());
			System.err.println(serviceInstance.toString());

			// 注册服务：自动注册/注销服务：断开连接自动注销服务，
			try (ServiceDiscovery<InstanceDetails> sd = ServiceDiscoveryBuilder
					.builder(InstanceDetails.class)
					.client(c)
					.basePath(path)
					.serializer(
							new JsonInstanceSerializer<InstanceDetails>(
									InstanceDetails.class))
					.thisInstance(serviceInstance)// 自动注册注销更新
					.build()) {
				sd.start();
				// switch (act) {
				// case 0:
				// sd.unregisterService(serviceInstance);// 手动注销
				// break;
				// case 1:
				// sd.registerService(serviceInstance);// 手动注册
				// break;
				// case 2:
				// sd.updateService(serviceInstance);// 手动更新
				// break;
				// default:
				// break;
				// }
				// 等待，连接断开会强制注销服务
				Thread.sleep(20000);
			}
		}
	}
	
	
	public static void testDiscoveryServer() throws Exception {
		
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("curator-log-events", "true");
		System.setProperty("curator-dont-log-connection-problem", "true");
		System.setProperty(
				"curator-log-only-first-connection-issue-as-error-level",
				"true");
		// try (CuratorExample test = new CuratorExample()) {
		// 启动，必须
		// test.client.start();
		// 启动cache，可选
		// test.initCache("/tmp/test");
		// test.cache.start();
		//
		// // 创建节点
		// test.create("/test");
		//
		// // 创建节点-包括父节点
		// test.createWithParent("/test/aa", "data".getBytes());
		//
		// // 读取数据
		// test.testReadData("/test/aa");
		//
		// // 创建临时节点
		// test.createTmpNode();
		//
		// // 锁
		// test.testLock();

		// // 共享计数器,非原子锁，非分布式锁，整数计数器，无自增功能
		// test.counter_ShareCount();
		//
		// // 乐观锁原子分布式计数器，long，有自己增加
		// test.counter_DistributedAtomicLong_Optimistic();
		// // 互斥锁原子分布式计数器，long，有自己增加
		// test.counter_DistributedAtomicLong_Mutex();
		// }
		// try (CuratorExample test = new CuratorExample()) {
		// test.client.start();//use TestingServer instead
		// leader 通知，执行任务
		// test.testLeader();
		// }
		// testLeaderLatch();
		// testSharedLock();
		// testSharedSemaphore();
		// testBarrier();
		// testDoubleBarrier();
		// testCounterShareCounter();
		// testDistributedAtomLong();
		// testPathCache();
		// testPersistentEphemeralNode();
		// testGroupMember();
		// testDistributeQueue();
		// testDistributeIDQueue();
		// testNamespaceClient();
		// testTempClient();
		// testCluster();
		new Thread(new Runnable() {
			public void run() {
				try {
					CuratorExample.testServiceDiscoveryRegister("/user", "add",
							2222, "user_add_service", 1);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();

		testServiceDiscoveryQuery("/user", "add");
		testServiceCacheQuery("/user", "add");
	}
}
