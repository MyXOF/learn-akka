package com.myxof.akka.iot;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;
import scala.concurrent.duration.FiniteDuration;

public class DeviceGroupQueryTest {

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
	public void testReturnTemperatureValueForWorkingDevices() {
		TestKit requester = new TestKit(system);

		TestKit device1 = new TestKit(system);
		TestKit device2 = new TestKit(system);

		Map<ActorRef, String> actorToDeviceId = new HashMap<>();
		actorToDeviceId.put(device1.getRef(), "device1");
		actorToDeviceId.put(device2.getRef(), "device2");

		ActorRef queryActor = system.actorOf(DeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
				new FiniteDuration(3, TimeUnit.SECONDS)));

		assertEquals(0L, device1.expectMsgClass(Device.ReadTemperature.class).requestId);
		assertEquals(0L, device2.expectMsgClass(Device.ReadTemperature.class).requestId);

		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(1.0)), device1.getRef());
		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(2.0)), device2.getRef());

		DeviceGroup.RespondAllTemperatures response = requester
				.expectMsgClass(DeviceGroup.RespondAllTemperatures.class);
		assertEquals(1L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", new DeviceGroup.Temperature(1.0));
		expectedTemperatures.put("device2", new DeviceGroup.Temperature(2.0));

		assertEquals(expectedTemperatures, response.temperatures);
	}

	@Test
	public void testReturnTemperatureNotAvailableForDevicesWithNoReadings() {
		TestKit requester = new TestKit(system);

		TestKit device1 = new TestKit(system);
		TestKit device2 = new TestKit(system);

		Map<ActorRef, String> actorToDeviceId = new HashMap<>();
		actorToDeviceId.put(device1.getRef(), "device1");
		actorToDeviceId.put(device2.getRef(), "device2");

		ActorRef queryActor = system.actorOf(DeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
				new FiniteDuration(3, TimeUnit.SECONDS)));

		assertEquals(0L, device1.expectMsgClass(Device.ReadTemperature.class).requestId);
		assertEquals(0L, device2.expectMsgClass(Device.ReadTemperature.class).requestId);

		queryActor.tell(new Device.RespondTemperature(0L, Optional.empty()), device1.getRef());
		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(2.0)), device2.getRef());

		DeviceGroup.RespondAllTemperatures response = requester
				.expectMsgClass(DeviceGroup.RespondAllTemperatures.class);
		assertEquals(1L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", DeviceGroup.TemperatureNotAvailable.INSTANCE);
		expectedTemperatures.put("device2", new DeviceGroup.Temperature(2.0));

		assertEquals(expectedTemperatures, response.temperatures);
	}

	@Test
	public void testReturnDeviceNotAvailableIfDeviceStopsBeforeAnswering() {
		TestKit requester = new TestKit(system);

		TestKit device1 = new TestKit(system);
		TestKit device2 = new TestKit(system);

		Map<ActorRef, String> actorToDeviceId = new HashMap<>();
		actorToDeviceId.put(device1.getRef(), "device1");
		actorToDeviceId.put(device2.getRef(), "device2");

		ActorRef queryActor = system.actorOf(DeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
				new FiniteDuration(3, TimeUnit.SECONDS)));

		assertEquals(0L, device1.expectMsgClass(Device.ReadTemperature.class).requestId);
		assertEquals(0L, device2.expectMsgClass(Device.ReadTemperature.class).requestId);

		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(1.0)), device1.getRef());
		device2.getRef().tell(PoisonPill.getInstance(), ActorRef.noSender());

		DeviceGroup.RespondAllTemperatures response = requester
				.expectMsgClass(DeviceGroup.RespondAllTemperatures.class);
		assertEquals(1L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", new DeviceGroup.Temperature(1.0));
		expectedTemperatures.put("device2", DeviceGroup.DeviceNotAvailable.INSTANCE);

		assertEquals(expectedTemperatures, response.temperatures);
	}

	@Test
	public void testReturnTemperatureReadingEvenIfDeviceStopsAfterAnswering() {
		TestKit requester = new TestKit(system);

		TestKit device1 = new TestKit(system);
		TestKit device2 = new TestKit(system);

		Map<ActorRef, String> actorToDeviceId = new HashMap<>();
		actorToDeviceId.put(device1.getRef(), "device1");
		actorToDeviceId.put(device2.getRef(), "device2");

		ActorRef queryActor = system.actorOf(DeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
				new FiniteDuration(3, TimeUnit.SECONDS)));

		assertEquals(0L, device1.expectMsgClass(Device.ReadTemperature.class).requestId);
		assertEquals(0L, device2.expectMsgClass(Device.ReadTemperature.class).requestId);

		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(1.0)), device1.getRef());
		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(2.0)), device2.getRef());
		device2.getRef().tell(PoisonPill.getInstance(), ActorRef.noSender());

		DeviceGroup.RespondAllTemperatures response = requester
				.expectMsgClass(DeviceGroup.RespondAllTemperatures.class);
		assertEquals(1L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", new DeviceGroup.Temperature(1.0));
		expectedTemperatures.put("device2", new DeviceGroup.Temperature(2.0));

		assertEquals(expectedTemperatures, response.temperatures);
	}

	@Test
	public void testReturnDeviceTimedOutIfDeviceDoesNotAnswerInTime() {
		TestKit requester = new TestKit(system);

		TestKit device1 = new TestKit(system);
		TestKit device2 = new TestKit(system);

		Map<ActorRef, String> actorToDeviceId = new HashMap<>();
		actorToDeviceId.put(device1.getRef(), "device1");
		actorToDeviceId.put(device2.getRef(), "device2");

		ActorRef queryActor = system.actorOf(DeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
				new FiniteDuration(3, TimeUnit.SECONDS)));

		assertEquals(0L, device1.expectMsgClass(Device.ReadTemperature.class).requestId);
		assertEquals(0L, device2.expectMsgClass(Device.ReadTemperature.class).requestId);

		queryActor.tell(new Device.RespondTemperature(0L, Optional.of(1.0)), device1.getRef());

		DeviceGroup.RespondAllTemperatures response = requester.expectMsgClass(java.time.Duration.ofSeconds(5),
				DeviceGroup.RespondAllTemperatures.class);
		assertEquals(1L, response.requestId);

		Map<String, DeviceGroup.TemperatureReading> expectedTemperatures = new HashMap<>();
		expectedTemperatures.put("device1", new DeviceGroup.Temperature(1.0));
		expectedTemperatures.put("device2", DeviceGroup.DeviceTimedOut.INSTANCE);

		assertEquals(expectedTemperatures, response.temperatures);
	}
}
