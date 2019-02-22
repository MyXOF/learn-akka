package com.myxof.akka.iot;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class IotSupervisor extends AbstractActor {
	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create("iot-system");
		try {
			ActorRef supervisor = system.actorOf(IotSupervisor.props(), "iot-supervisor");
		} finally {
			system.terminate();
		}
	}

	public static Props props() {
		return Props.create(IotSupervisor.class, IotSupervisor::new);
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder().build();
	}

	@Override
	public void preStart() {
		log.info("IoT application started");
	}
	
	@Override
	public void postStop() {
		log.info("IoT application stopped");
	}
	
	
}
