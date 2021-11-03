Basic Design
============

With version **2**, disBatch consists of three major components:

* A driver (aka _controller_) that maintains the state of the task processing.
* An execution context that encapsulates one or more engines running on one or mode nodes. A disBatch run may have multiple contexts. 
* An engine that is a collection of cylinder threads. Each cylinder runs a loop that waits for a task from the controller, spawns a sub-process to evaluate it, waits for the sub-process to exit, and then sends a report to the controller.


Driver
-----

In normal operation, the driver spawns a couple of threads. One implements that KVS service. Another is the task feeder. This takes tasks from a task generator and hands them off to the controller via KVS.

Each task has an age, which reflects the number of synchronization events that preceded it. Synchronization events are barriers and per engine tasks. Per engine tasks are posted to KVS. A barrier is tracked by the controller. When all tasks prior to the barrier have been completed, the barrier is satisfied, a per engine event to this effect is posted to KVS and the controller's age is increased. The controller notifies the task feeder thread of the new age. The task feeder will not issue a task to the controller unless the controller's age is equal to the task's age. This interplay ensures no task is available for execution until all previous barriers (and thus in turn, all previous tasks) have been accounted for. Something akin to this takes place between an engine and its cylinders to implement per engine synchronization. See below.

The controller executes a main loop that waits for a controller event to arrive from KVS. These events include a new task from the task feeder, a completed task report from a cylinder, the registration of an execution context or an engine, a cylinder start, the notification that a context, engine or cylinder has stopped, requests to shutdown a context or a cylinder, and a few other events.

Each pass through the loop, the controller:

- Accepts a controller message from KVS. These may lead it to alter its internal state (say add a new cylinder) or execute an operation like sending a shutdown message to an engine. Of particular interest are messages providing a new task to execute, which causes that task to be added to a list of tasks with the same age, and messages reporting the completion of a task, which causes the cylinder it was assigned to to be marked available again and the finished task id to be recorded.
- Checks to see if all necessary tasks have been completed to satisfy a barrier. If so the age is advanced, and other barriers iteratively checked---that is the completion of one task could in effect satisfy a series of successive barriers.
- If there are tasks for the current age and available cylinders, assign tasks to the individual cylinders until we run out of one or the other. **Note:** If we record the assignments (including the full task), it should be straightforward to reissue tasks upon engine "failure".
- Update overall status info that is kept in KVS. This is used by `dbmon.py` to provide quasi-realtime info about the state of a disBatch run.

As noted, the driver receives messages informing it of new contexts, engines and cylinders. A portion of this information is incorporated in the status report placed in KVS. It is also used to implement task limits for contexts. Once the controller has assigned the cylinder(s) of the engine(s) of a context a total number of tasks equal to the task limit specified for the context, it sends a shutdown request to every engine in the context.

Execution context
-----------------

A context is responsible for interfacing between a collection of computational resources and a controller. Currently two kinds are supported:

* SLURM: This context makes use of environment variables set by SLURM to identify the allocated nodes and uses `srun` to start engines. The code here could serve as a model for implementing contexts for other batch queuing systems.
* SSH: The nodes to be used are passed via the command line option (`-s`) or the environment variable `DISBATCH_SSH_NODELIST`. Engines are started via `ssh`.

Each context monitors its engines and invokes a retirement method, if provided, when an engine exits.

A context is also a logical shutdown unit. The user can, for example via `dbmon.py`, request that a context be shutdown. This is implemented by sending a shutdown request to each of the context's engines. **Note:** Such a request waits politely for all cylinders to complete any currently assigned tasks before stopping the engine.


Engine
------

An engine is a collection of N+1 cylinder threads, where N is the number of allowable concurrently executing tasks specified for the engine. The extra cylinder handles the per-engine tasks. Per-engine tasks are maintained as an ordered queue in KVS: engines `view` values using a key with an index, stepping the index each time. Thus an engine joining at any given time can "replay" all the per engine activity. As it does so, it updates its internal age, and notifies each of its cylinders of the current age. A cylinder will not execute an assigned task until the engine has reached that task's age.


Use modes
---------

With the exception of some reporting details, the "standard" case should be the same as with version **1**.

With version **2**, a user can invoke `disBatch` with `-S`, which starts a disBatch "service"---effectively just the controller. In this case, the name of a utility script is displayed. This script (always created by version **2**), can be submitted via sbatch to add an execution context. One could even submit this with a job array specification, and so add nodes on the fly to the disBatch run. The same script can be invoked with `-s` to add some ssh hosts to the mix, e.g., the user's own workstation.

The script can be invoked with `--mon` to start up a simple ASCII-UI to monitor progress and request shutdown of an engine or a context.

Comments
--------
1. The controller is supposed to be the only single point of failure, nothing else (in the disBatch system) should be (assuming non malicious failure). Barriers (including an implicit one at the end), of course, might not be satisfied, but that aside a disBatch run can keep going even if a context or engine dies (if all engines died, more would have to be added to make more progress).

2. Idempotency and task reissue.

3. cli version of dbmon.py.

4. Job array demo. (Theory vs practice.)

5. Add option to insert `timeout`?

6. Add heartbeat as a failure detection mechanism?

7. pernode vs perengine

8. Remove delay for explicitly started engines? Probably not ...

Messages communicated through KVS
---------------------------------

.common_env:
------------
{'DISBATCH_JOBID':, 'DISBATCH_NAMETASKS':}
	post by Driver when it is created

.controller 
-----------
('register', ('context/engine', random_generted_key)):
	post by disBatch --context or --engine 
        update context/engine count and put (radom_generated_key, count)
('context info', context_instance):
        post by BatchContext when launching a new context
        update Driver's information
('stop context', context_rank):
        shutdown engines of the context
('engine started', (self.rank, context.rank, myHostname, myPid, ts)):
	post by EngineBlock when a context starts an engine on a node
	create a engine proxy and update driver's information
('engine stopped', ('OK', self.rank)/(exception, self.rank)):
	post by EngineBlock when it is done ('OK', self.rank) or has an exception (exception, self.rank)
	update driver's information
('stop engine', engine_rank):
	shutdown an engine with rank
('cylinder available', (self.engineRank, self.pid, self.pgid, self.key='.cylinder {engineRank} {cylinderId}')):
	post by Cylinder when it is started
	update driver's information
('cylinder stopped', (self.engineRank, self.key)):
	post by Cylinder when it receives '.cylinder {engineRank} {cylinderId}:stop'
	update driver's information

('special task', task_info):
	post by Feeder when task_info.kind is one of 'BCD'
	deal with aferwards considering the task is finished
('task', task_info):
	post by Feeder when task_info.kind is one of 'NPS'
	if "per engine task", post '{taskKey} task'
	elif "skipping", post '.controller', 'task skipped'
	else add to Driver's pending queue
('task skipped', task_info)
        post by Driver for 'S' task when receiving '.controller:task'
        afterwards recording of the task
('task done', (task_report, self.engineRank, self.cylinderId, age, self.key)):
	post by Cylinder
	afterwards after a task (or a zombie task) is done
('feeder exception', None):
	post by Feeder
('no more tasks', last_task_id+1):
	post by Feeder when there is no more tasks
	bookkeeping

('driver heart beat', None):
	put by disBatch driver loop
        check for task heartbeat fail, put .controller, 'task hb fail'
('task heart beat', {task_id}'):
	post by Cylinder for a task
        update Driver information
('task hb fail', (task_info, cylinder_key, start, ts)):
	post by Driver for outstanding tasks when receiving a 'driver heart beat' message
        consider the task is done, set hbFail and generate TaskReport
('clearing barriers', None)
	post by Driver

'.db info'
----------
disbatch_info:
	post by disBatch
 
'Disbatch status'
-----------------
status_json_str:
	post by Driver, such as '<Starting...>'

'.context_{rank}'
-----------------
context_instance:
	post by Context when it launches

'{context_key}'
---------------
context_count:
	post by Driver when receiving '.controller:register'

'{engine_key}'
--------------
engine_count:
	post by Driver when receiving '.controller:register'

'engine {rank}'
---------------
'running':
	post by EngineBlock when it starts
'engine stopped':
	post by EngineBlock when it is finished

'.cylinder {engineRank} {cylinderId}'
------------
('stop', None)
	post by EngineProxy when the the engine stops
	post by Driver when there is no more task
('task', task_info)
	post by Driver to assign pending tasks

'{task_source_name} task'
---------------------------
'{task_source_name} done!':
	post by KVSTaskSource when all tasks are done

'{task_key}'
------------
('task', task_info):
	post by Driver for 'P' task when receiving '.controler:task'

'{barrier_key}'
--------------
'{barrier_task_id'}
	post by Driver to signal the barrier is done

'{barrier_task_key}'
-------------------
('barrier_notification', barrier_info)
	post by Driver

'{track_results}{task_id}'
------------------------
task_report:
	post by Driver
