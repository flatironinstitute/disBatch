Key value storage server
========================

Inspired by networkspaces, which was in turn inspired by the Linda coordination language.

Similar systems exist, the point of this one is to provide a simple to deploy and reasonably functional and efficient store that is easy to integrate with many different programming environments.

The reference python implementation should work with any stock python 2.7 or above:

   * `kvscommon.py` contains the line protocol description and common utilities,
   * `kvsstcp.py` contains the server, which can be run from the command line or from within another python module as `KVSServer()` to start the server thread
   * `kvsclient.py` contains the client interface, which can be run from the command line or from within another python module as `KVSClient(host, port)`

"kvsSupport.[ch]" contains a client that can be linked with C or FORTRAN codes.

"kvsTest.py" provides a simple example of use.

"kvsRing.py" can be used to generate some basic timing information.

"kvsLoop.c" and "kvsTestf.f" are example codes for C and FORTRAN. "Makefile" can be used to build these.

"kvsBatchWrapper.sh" is a short script to invoke a program that uses KVS via a SLURM sbatch submission, e.g.:

	sbatch -N 2 --ntasks-per-node=28 --exclusive kvsBatchWrapper.sh ./kvsTestf

`wskvsmu.py` is a prototype web interface for displaying the state of a KVS server (and injecting values into it). Uses `wskvspage.html` as the frontend.

"kvsTestWIc.c" and "kvsTestWIf.f" provide example codes that use KVS via wskvsmu.py to enter input from a web browser into C or FORTRAN. 

## License

Copyright 2017 Simons Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
