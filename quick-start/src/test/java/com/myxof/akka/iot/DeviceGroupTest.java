package com.myxof.akka.iot;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;

public class DeviceGroupTest {

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
	public void testRegisterDeviceActor() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor1 = probe.getLastSender();

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor2 = probe.getLastSender();
		assertNotEquals(deviceActor1, deviceActor2);

		// Check that the device actors are working
		deviceActor1.tell(new Device.RecordTemperature(0L, 1.0), probe.getRef());
		assertEquals(0L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);
		deviceActor2.tell(new Device.RecordTemperature(1L, 2.0), probe.getRef());
		assertEquals(1L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);
	}

	@Test
	public void testIgnoreRequestsForWrongGroupId() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("wrongGroup", "device1"), probe.getRef());
		probe.expectNoMessage();
	}

	@Test
	public void testReturnSameActorForSameDeviceId() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor1 = probe.getLastSender();

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor2 = probe.getLastSender();
		assertEquals(deviceActor1, deviceActor2);
	}

	@Test
	public void testListActiveDevices() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);

		groupActor.tell(new DeviceGroup.RequestDeviceList(0L), probe.getRef());
		DeviceGroup.ReplyDeviceList reply = probe.expectMsgClass(DeviceGroup.ReplyDeviceList.class);
		assertEquals(0L, reply.requestId);
		assertEquals(Stream.of("device1", "device2").collect(Collectors.toSet()), reply.ids);
	}

	@Test
	public void testListActiveDevicesAfterOneShutsDown() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef toShutDown = probe.getLastSender();

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);

		groupActor.tell(new DeviceGroup.RequestDeviceList(0L), probe.getRef());
		DeviceGroup.ReplyDeviceList reply = probe.expectMsgClass(DeviceGroup.ReplyDeviceList.class);
		assertEquals(0L, reply.requestId);
		assertEquals(Stream.of("device1", "device2").collect(Collectors.toSet()), reply.ids);

		probe.watch(toShutDown);
		toShutDown.tell(PoisonPill.getInstance(), ActorRef.noSender());
		probe.expectTerminated(toShutDown);

		// using awaitAssert to retry because it might take longer for the groupActor
		// to see the Terminated, that order is undefined
		probe.awaitAssert(() -> {
			groupActor.tell(new DeviceGroup.RequestDeviceList(1L), probe.getRef());
			DeviceGroup.ReplyDeviceList r = probe.expectMsgClass(DeviceGroup.ReplyDeviceList.class);
			assertEquals(1L, r.requestId);
			assertEquals(Stream.of("device2").collect(Collectors.toSet()), r.ids);
			return null;
		});
	}

	@Test
	public void testCollectTemperaturesFromAllActiveDevices() {
		TestKit probe = new TestKit(system);
		ActorRef groupActor = system.actorOf(DeviceGroup.props("group"));

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor1 = probe.getLastSender();

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor2 = probe.getLastSender();

		groupActor.tell(new DeviceManager.RequestTrackDevice("group", "device3"), probe.getRef());
		probe.expectMsgClass(DeviceManager.DeviceRegistered.class);
		ActorRef deviceActor3 = probe.getLastSender();

		// Check that the device actors are working
		deviceActor1.tell(new Device.RecordTemperature(0L, 1.0), probe.getRef());
		assertEquals(0L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);
		deviceActor2.tell(new Device.RecordTemperature(1L, 2.0), probe.getRef());
		assertEquals(1L, probe.expectMsgClass(Device.TemperatureRecorded.class).requestId);
		// No temperature for device 3

		groupActor.tell(new DeviceGroup.RequestAllTemperatures(0L), probe.getRef());
		DeviceGroup.RespondAllTemperatures response = probe.expectMsgClass(DeviceGroup.RespondAllTemperatures.class);
		assertEquals(0L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", new DeviceGroup.Temperature(1.0));
		expectedTemperatures.put("device2", new DeviceGroup.Temperature(2.0));
		expectedTemperatures.put("device3", DeviceGroup.TemperatureNotAvailable.INSTANCE);

		assertEquals(expectedTemperatures, response.temperatures);
	}
}
