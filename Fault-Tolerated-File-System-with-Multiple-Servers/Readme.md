Used Python to integrate a file system with Multiple Replica Servers for performance and fault tolerance. A repair mechanism for correcting corrupted data was developed in for compromised servers.

Here I only show the main part of the project: the mediator server which connects between the client and all data servers. For replicated state machines to provide increased fault tolerance, the replicas should fail independently. Here we use the quorum approach to
enhance the system. This approach defines read and write quorums Qr and Qw separately, and it sets Qr + Qw > Nreplicas and Qw = Nreplicas.
