# A simulated client && database server for client-side direct WAL write evaluation

## Key Class Descriptions

1. **SimulatedDatabaseServer**  
   A simulated database server, where each process represents a single partition. It listens on a specified port and receives WAL Entry requests (here it is assumed the client sends a WAL Entry as a database write request; since the WAL Entry contains all actual write request content, this simulation is feasible). Upon receiving a request, multiple worker threads process it. The worker threads generate a monotonically increasing p-sn, construct a p-cp-map-msg, and return it to the sender.

2. **SimulatedClient**  
   A simulated client composed of multiple worker threads that periodically generate WAL Entries and concurrently write them to both the database server and HDFS. When responses are received from both the database server and HDFS, the request is considered to have entered the Returnable stage; the elapsed time from the start of the write to entering the Returnable stage is recorded. Subsequently, the p-cp-map-msg is concurrently written to HDFS.

3. **FanOutOneBlockAsyncDFSOutput**  
   An implementation of a quorum write scheme for HDFS, achieving "3 out of 2": as soon as 2 out of 3 DataNodes in the cluster have written the data to their page cache, the HDFS write is considered persisted.

## Deploying SimulatedDatabaseServer and SimulatedClient

Before compiling, modify variables in the code as needed, such as the number of threads, port numbers, and the simulated database processing logic (currently implemented as a sleep; it can be changed to writing a data block to disk or a CPU-intensive computation task), as well as the statistics logic (currently, only simple timing is performed; in practice, it should be configured based on the local statistics and visualization tools used, such as ganglia + grafana).
