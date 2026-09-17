A project for area 14, "Parallel replication and replication lag" for the [MariaDB student database projects, 2026-09](https://mariadb.org/bachelor_hackathon_2026-09/#tier-a-start-here).

# MariaDB Parallel Replication Lab

A reproducible laboratory for studying parallel replication and replication lag in MariaDB.

## Project

This repository is being developed for the Constructor University Databases Project 2026.

The goal is to create a write workload that causes a single-threaded MariaDB replica to fall behind its primary, and then investigate how parallel replication affects replication lag under different workloads and configurations.

## Research Questions

1. How does the number of replication worker threads affect replication lag?
2. At what point does increasing parallelism stop providing a significant improvement?
3. Which transaction workloads parallelize well, and which workloads limit replication parallelism?

## Planned Setup

- MariaDB primary
- MariaDB replica
- Docker Compose for reproducible deployment
- Python workload generator
- Replication lag monitor
- Automated experiment runner
- CSV result collection and plots

## MariaDB Version

MariaDB 11.8.x LTS.

The exact patch version used for the final experiments will be pinned and documented.

## Status

Initial setup and replication environment configuration.

## Team

ReplicaLab
- Taizhanov Adilbek (ataizhanov@constructor.university)
