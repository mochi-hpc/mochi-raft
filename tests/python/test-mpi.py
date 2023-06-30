import sys
import random
from mpi4py import MPI
from pymargo.core import Engine
import pymargo.core
from pymraft import ServerInfo, Raft
import pymraft


class MyStateMachine():

    def __init__(self):
        self.content = b''

    def apply(self, command: memoryview) -> None:
        command = bytes(command)
        print(f"Received command {command}")
        self.content += command + b'-'

    def snapshot(self) -> bytes:
        return self.content

    def restore(self, snapshot: memoryview) -> None:
        self.content = bytes(snapshot)


def test(engine: Engine, servers: list[ServerInfo]):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    fsm = MyStateMachine()
    raft = Raft(mid=engine.mid, id=rank+1, fsm=fsm)
    raft.bootstrap(servers)
    comm.barrier()
    raft.start()
    comm.barrier()
    print("Starting to apply commands")
    #for i in range(0, 10):
    #    engine.sleep(random.randint(1, 200))
    #    command = f"{rank}/{i}".encode("ascii")
    #    raft.apply(command)
    print("Sleeping to make sure everyone finished")
    engine.sleep(2000)

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    random.seed(rank)
    with Engine('ofi+tcp', mode=pymargo.core.server, use_progress_thread=True) as engine:
        engine.logger.set_log_level(pymargo.logging.level.trace)
        self_addr = str(engine.addr())
        addresses = comm.allgather(self_addr)
        servers = [ServerInfo(id=i+1, address=address, role=pymraft.role.Voter)
                   for i, address in enumerate(addresses)]
        test(engine, servers)
