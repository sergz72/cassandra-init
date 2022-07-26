# cassandra-init

This application allows to init Cassandra database using init scripts.

Usage: cassandra-init db_name init_scripts_folder

Keyspace db_name will be created and all cql scripts from init_scripts_folder will be executed sequentially in alphabetical order.
