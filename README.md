# Mochi-RAFT

This repository provides an implementation of the RAFT protocol using
Margo for communication and [C-Raft](https://github.com/canonical/raft)
for the protocol itself.

Mochi-RAFT (Mraft) is modular and allows users to provide their own
implementation of a persistent log.
