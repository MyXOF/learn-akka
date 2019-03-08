package cn.edu.myxof.distributed;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.DistributedData;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ORMultiMap;
import akka.cluster.ddata.ORSet;
import akka.cluster.ddata.ORSetKey;
import akka.cluster.ddata.PNCounterMap;
import akka.cluster.ddata.Replicator;
import akka.cluster.ddata.Replicator.Changed;
import akka.cluster.ddata.Replicator.Subscribe;
import akka.cluster.ddata.Replicator.Update;
import akka.cluster.ddata.Replicator.UpdateResponse;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class DataBot extends AbstractActor {
	private static final String TICK = "tick";

	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
	private final Cluster node = Cluster.get(getContext().getSystem());

	private final Cancellable tickTask = getContext().getSystem().scheduler().schedule(Duration.ofSeconds(5),
			Duration.ofSeconds(5), getSelf(), TICK, getContext().getDispatcher(), getSelf());

	private final Key<ORSet<String>> dataKey = ORSetKey.create("key");

	static public Props props() {
		return Props.create(DataBot.class, () -> new DataBot());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(String.class, a -> a.equals(TICK), a -> receiveTick())
				.match(Changed.class, c -> c.key().equals(dataKey), c -> receiveChanged((Changed<ORSet<String>>) c))
				.match(UpdateResponse.class, r -> receiveUpdateResponse(r))
				.build();
	}

	private void receiveTick() {
		String s = String.valueOf((char) ThreadLocalRandom.current().nextInt(97, 123));
		if (ThreadLocalRandom.current().nextBoolean()) {
			// add
			log.info("Adding: {}", s);
			Update<ORSet<String>> update = new Update<>(dataKey, ORSet.create(), Replicator.writeLocal(),
					curr -> curr.add(node.selfUniqueAddress(), s));
			replicator.tell(update, getSelf());
		} else {
			// remove
			log.info("Removing: {}", s);
			Update<ORSet<String>> update = new Update<>(dataKey, ORSet.create(), Replicator.writeLocal(),
					curr -> curr.remove(node.selfUniqueAddress(), s));
			replicator.tell(update, getSelf());
		}
	}

	private void receiveChanged(Changed<ORSet<String>> c) {
		log.info("Current keys: {}", c.key());
		ORSet<String> data = c.dataValue();
		log.info("Current elements: {}", data.getElements());
	}

	private void receiveUpdateResponse(UpdateResponse<?> r) {
		// ignore
		log.info("Recevie Update response {}", r);
	}

	@Override
	public void preStart() {
		Subscribe<ORSet<String>> subscribe = new Subscribe<>(dataKey, getSelf());
		replicator.tell(subscribe, ActorRef.noSender());
	}

	@Override
	public void postStop() {
		tickTask.cancel();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Config config = ConfigFactory.parseString("akka.actor.provider=cluster").withFallback(ConfigFactory.load());
		ActorSystem system = ActorSystem.create("Demo", config);
		system.actorOf(DataBot.props(), "DataBot-Demo");
//		final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
//		final PNCounterMap<String> m0 = PNCounterMap.create();
//		final PNCounterMap<String> m1 = m0.increment(node, "a", 7);
//		final PNCounterMap<String> m2 = m1.decrement(node, "a", 2);
//		final PNCounterMap<String> m3 = m2.increment(node, "b", 1);
//		System.out.println(m3.get("a")); // 5
//		System.out.println(m3.getEntries());
//		final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
//		final ORMultiMap<String, Integer> m0 = ORMultiMap.create();
//		final ORMultiMap<String, Integer> m1 = m0.put(node, "a", new HashSet<>(Arrays.asList(1, 2, 3)));
//		final ORMultiMap<String, Integer> m2 = m1.addBinding(node, "a", 4);
//		final ORMultiMap<String, Integer> m3 = m2.removeBinding(node, "a", 2);
//		final ORMultiMap<String, Integer> m4 = m3.addBinding(node, "b", 1);
//		System.out.println(m4.getEntries());
	}

}
