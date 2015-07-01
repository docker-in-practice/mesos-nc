from __future__ import print_function

import os
import sys
import signal
import threading

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

# The port ranges we want to use in this framework
minport = 8000
maxport = 8005

# Resource requirements for any task we start
CPUS_REQUIRED = 0.1
MEM_REQUIRED = 16

class TestScheduler(mesos.interface.Scheduler):

    # On startup, initialise the number of running tasks.
    def __init__(self):
        self._numtasks = 0

    # Mesos will allocate a framework id when the scheduler has registered with
    # the mesos master.
    # We don't use this for anything, so just print it out.
    def registered(self, driver, frameworkId, masterInfo):
        print("Registered with framework ID {}".format(frameworkId.value))

    # Mesos will let our framework know whenever a task starts and stops.
    # We don't need to act on this information, so just print it out.
    def statusUpdate(self, driver, update):
        print("Task {} is in state {}".format(
            update.task_id.value, mesos_pb2.TaskState.Name(update.state)
        ))

    # Helper function!
    # Putting together the required information to define a task is simple, but
    # lengthy. Keeping it as a helper function makes it easier to see what's going on.
    def _makeTask(self, tid, slave_id_value, port):
        print("Creating task " + str(tid))

        # The essentials for defining a task.
        # The task id can be any string unique across all tasks.
        # The slave id is the id of the slave to start the task on (taken from
        # the detail of the offer).
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = slave_id_value
        task.name = "task %d" % tid

        # https://github.com/apache/mesos/blob/2985ae05634038b70f974bbfed6b52fe47231418/include/mesos/mesos.proto#L992
        # This is where Docker comes in!
        # All of this section of code is dedicated to passing the correct
        # settings to docker i.e. the image we want to use and port mappings
        task.container.type = task.container.DOCKER
        task.container.docker.image = 'ubuntu:14.04.2'
        task.container.docker.network = task.container.docker.BRIDGE
        portmapping = task.container.docker.port_mappings.add()
        portmapping.host_port = portmapping.container_port = port
        portmapping.protocol = 'tcp'

        # If we commented out all of the docker details above, this task
        # would still work because of the crucial line below telling the task
        # what it actually needs to execute - it just wouldn't run inside a
        # container.
        task.command.value = "echo 'hello {}' | nc -l {}".format(tid, port)

        # Mesos is a resource usage enforcer, so we need to tell it what
        # resources we want when running our task. This includes CPU and memory,
        # but also ports - they too are a resource that can be exhausted!
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = CPUS_REQUIRED

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = MEM_REQUIRED

        ports = task.resources.add()
        ports.name = "ports"
        ports.type = mesos_pb2.Value.RANGES
        portrange = ports.ranges.range.add()
        portrange.begin = port
        portrange.end = port

        return task

    # Mesos will let our framework know about resource offers when they become
    # available.
    # We will act on these by accepting any offer that fits our requirement for
    # resources (ports, memory, cpu).
    def resourceOffers(self, driver, offers):

        # Consider each offer in turn to find and accept all suitable offers.
        # Note that we will only ever launch one task per offer, even if it
        # would be possible to launch multiple - this isn't a problem as mesos
        # will come back and let us know about updated resource availability,
        # giving us a chance to launch more tasks.
        for offer in offers:
            offerCpus = 0
            offerMem = 0
            offerPorts = []
            # Each offer contains information on resources available - tally
            # them up!
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
                elif resource.name == "ports":
                    selectedPort = None
                    # Mesos represents port resource offers as a number of
                    # ranges so consider each range in turn.
                    for portrange in resource.ranges.range:
                        offerPorts.append("{}-{}".format(portrange.begin, portrange.end))
                        if selectedPort:
                            continue
                        # If any of the possible ports are within the offered
                        # port range, select it.
                        for possiblePort in range(minport, maxport+1):
                            if portrange.begin <= possiblePort <= portrange.end:
                                selectedPort = possiblePort
                                break

            print("Received offer {}. cpus: {}, mem: {}, ports: {}".format(
                offer.id.value, offerCpus, offerMem, ",".join(offerPorts)
            ))

            # Declining an offer indicates to mesos that the offer is of no use.
            # These resources will not be offered back to the framework for a
            # period of time (currently defaults as 5s).
            if offerCpus < CPUS_REQUIRED or offerMem < MEM_REQUIRED or selectedPort is None:
                print("Declining offer")
                driver.declineOffer(offer.id)
                continue

            # Keep track of the number of launched tasks in order to give each
            # task a unique id.
            tid = self._numtasks
            self._numtasks += 1

            # Use the helper function to create a task object, and ask mesos to
            # launch it.
            tasks = [self._makeTask(tid, offer.slave_id.value, selectedPort)]
            driver.launchTasks(offer.id, tasks)

driver = None
driver_thread = None

# Make sure that mesos correctly shuts down when Ctrl+C is pressed.
def signal_handler(signal, frame):
    print("Got Ctrl+C, quitting")
    if driver_thread is not None:
        driver.stop()
        driver_thread.join()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <master:port>".format(sys.argv[0]))
        sys.exit(1)
    masterUrl = sys.argv[1]

    # Put together information about the scheduler and framework for use by mesos
    scheduler = TestScheduler()
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    driver = mesos.native.MesosSchedulerDriver(scheduler, framework, masterUrl)
    # Start the framework and wait for Ctrl+C
    driver_thread = threading.Thread(target=driver.run)
    driver_thread.start()
    signal.pause()
