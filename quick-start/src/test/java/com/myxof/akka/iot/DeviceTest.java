package com.myxof.akka.iot;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;

public class DeviceTest {
	static ActorSystem system;

	@Before
	public void setUp() throws Exception {
		system = ActorSystem.create();
	}

	@After
	public void tearDown() throws Exception {
		TestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testCreateReceive() {
		TestKit probe = new TestKit(system);
		ActorRef deviceActor = system.actorOf(Device.props("group", "device"));
		deviceActor.tell(new Device.ReadTemperature(42L), probe.getRef());
		Device.RespondTemperature response = probe.expectMsgClass(Device.RespondTemperature.class);
		assertEquals(42L, response.requestId);
		assertEquals(Optional.empty(), response.value);
	}

	@Test
	public void testReplyWithLatestTemperatureReading() {
		TestKit probe = new TestKit(system);
		ActorRef deviceActor = system.actorOf(Device.props("group", "device"));

		deviceActor.tell(new Device.RecordTemperature(1L, 24.0), probe.getRef());
		assertEquals(1L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);

		deviceActor.tell(new Device.ReadTemperature(2L), probe.getRef());
		Device.RespondTemperature response1 = probe.expectMsgClass(Device.RespondTemperature.class);
		assertEquals(2L, response1.requestId);
		assertEquals(Optional.of(24.0), response1.value);

		deviceActor.tell(new Device.RecordTemperature(3L, 55.0), probe.getRef());
		assertEquals(3L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);

		deviceActor.tell(new Device.ReadTemperature(4L), probe.getRef());
		Device.RespondTemperature response2 = probe.expectMsgClass(Device.RespondTemperature.class);
		assertEquals(4L, response2.requestId);
		assertEquals(Optional.of(55.0), response2.value);
	}

	@Test
	public void testReplyToRegistrationRequests() {
		TestKit probe = new TestKit(system);
		ActorRef deviceActor = system.actorOf(Device.props("group", "device"));

		deviceActor.tell(new DeviceManager.RequestTrackDevice("group", "device"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		assertEquals(deviceActor, probe.getLastSender());
	}

	@Test
	public void testIgnoreWrongRegistrationRequests() {
		TestKit probe = new TestKit(system);
		ActorRef deviceActor = system.actorOf(Device.props("group", "device"));

		deviceActor.tell(new DeviceManager.RequestTrackDevice("wrongGroup", "device"), probe.getRef());
		probe.expectNoMessage();

		deviceActor.tell(new DeviceManager.RequestTrackDevice("group", "wrongDevice"), probe.getRef());
		probe.expectNoMessage();
	}
}
