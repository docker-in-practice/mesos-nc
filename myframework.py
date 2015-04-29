from __future__ import print_function

import os
import sys
import signal
import threading

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

baseport = 8000
maxport = 8005

CPUS_REQUIRED = 0.1
MEM_REQUIRED = 20

class TestScheduler(mesos.interface.Scheduler):

    def __init__(self):
        self._numtasks = 0

    def registered(self, driver, frameworkId, masterInfo):
        print("Registered with framework ID {}".format(frameworkId.value))

    def statusUpdate(self, driver, update):
        print("Task {} is in state {}".format(
            update.task_id.value, mesos_pb2.TaskState.Name(update.state)
        ))

    def _makeTask(self, tid, slave_id_value, port):
        print("Creating task " + str(tid))

        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = slave_id_value
        task.name = "task %d" % tid

        # https://github.com/apache/mesos/blob/2985ae05634038b70f974bbfed6b52fe47231418/include/mesos/mesos.proto#L992
        task.container.type = task.container.DOCKER
        task.container.docker.image = 'ubuntu:14.04.2'
        task.container.docker.network = task.container.docker.BRIDGE
        portmapping = task.container.docker.port_mappings.add()
        portmapping.host_port = portmapping.container_port = port
        portmapping.protocol = 'tcp'

        task.command.value = "echo 'hello {}' | nc -l {}".format(tid, port)

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

    def resourceOffers(self, driver, offers):
        for offer in offers:
            offerCpus = 0
            offerMem = 0
            offerPorts = []
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
                elif resource.name == "ports":
                    selectedPort = None
                    for portrange in resource.ranges.range:
                        offerPorts.append("{}-{}".format(portrange.begin, portrange.end))
                        if selectedPort:
                            continue
                        # Select a port in the range we've defined as acceptable
                        for possiblePort in range(baseport, maxport+1):
                            if portrange.begin <= possiblePort <= portrange.end:
                                selectedPort = possiblePort
                                break

            print("Received offer {}. cpus: {}, mem: {}, ports: {}".format(
                offer.id.value, offerCpus, offerMem, ",".join(offerPorts)
            ))

            if offerCpus < CPUS_REQUIRED or offerMem < MEM_REQUIRED or selectedPort is None:
                print("Declining offer")
                driver.declineOffer(offer.id)
                continue

            tid = self._numtasks
            self._numtasks += 1

            tasks = [self._makeTask(tid, offer.slave_id.value, selectedPort)]
            driver.launchTasks(offer.id, tasks)

driver = None
driver_thread = None

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

    scheduler = TestScheduler()
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    driver = mesos.native.MesosSchedulerDriver(scheduler, framework, masterUrl)
    driver_thread = threading.Thread(target=driver.run)
    driver_thread.start()
    signal.pause()
