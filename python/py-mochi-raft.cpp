#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <mochi-raft.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t", nullptr)

#define CHECK_RET_AND_RAISE(ret, func) do { \
    if(ret != 0) { \
        throw std::runtime_error( \
            std::string{#func " failed  with error code "} + std::to_string(ret)); \
    } \
    } while(0)

struct PyStateMachine : public mraft::StateMachine {

    py::object m_fsm;

    PyStateMachine(py::object fsm)
    : m_fsm(std::move(fsm)) {}

    void apply(const struct raft_buffer *buf, void **result) override {
        try {
            auto mem = py::memoryview::from_memory((void*)buf->base, buf->len, true);
            m_fsm["apply"](mem);
        } catch(py::error_already_set& ex) {
            throw mraft::RaftException(911, ex.what());
        }
    }

    void snapshot(struct raft_buffer *bufs[], unsigned *n_bufs) override {
        try {
            py::buffer buffer = m_fsm["snapshot"]();
            auto buf_info = buffer.request(false);
            bool is_contiguous = true;
            ssize_t stride = (buf_info).itemsize;
            for(ssize_t i=0; i < buf_info.ndim; i++) {
                if(stride != buf_info.strides[i])
                    is_contiguous = false;
                stride *= buf_info.shape[i];
            }
            // TODO support non-contiguous memory
            if(!is_contiguous)
                throw mraft::RaftException(911, "snapshot provided non-contiguous memory");
            *bufs = (raft_buffer *)raft_calloc(1, sizeof(*bufs));
            (*bufs)[0].base = buf_info.ptr;
            (*bufs)[0].len  = buf_info.size * buf_info.itemsize;
        } catch(py::error_already_set& ex) {
            throw mraft::RaftException(911, ex.what());
        }
    }

    void restore(struct raft_buffer *buf) override {
        try {
            auto mem = py::memoryview::from_memory((void*)buf->base, buf->len, true);
            m_fsm["restore"](mem);
        } catch(py::error_already_set& ex) {
            throw mraft::RaftException(911, ex.what());
        }
    }

};

struct PyRaft {

    mraft::MemoryLog m_log;
    PyStateMachine   m_fsm;
    mraft::Raft      m_raft;

    PyRaft(py_margo_instance_id mid,
           raft_id id,
           py::object fsm,
           uint16_t provider_id)
    : m_fsm(std::move(fsm))
    , m_raft(mid, id, m_fsm, m_log, provider_id, ABT_POOL_NULL) {}

#define CHECK_FOR_PYTHON_EXCEPTION do { \
    if(PyErr_Occurred()) {              \
        throw py::error_already_set();  \
    }                                   \
} while(0)

    void bootstrap(const std::vector<mraft::ServerInfo>& servers) {
        m_raft.bootstrap(servers);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void recover(const std::vector<mraft::ServerInfo>& servers) {
        m_raft.recover(servers);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void apply(const py::buffer& buffer) {
        auto buf_info = buffer.request();
        bool is_contiguous = true;
        ssize_t stride = (buf_info).itemsize;
        for(ssize_t i=0; i < buf_info.ndim; i++) {
            if(stride != buf_info.strides[i])
                is_contiguous = false;
            stride *= buf_info.shape[i];
        }
        // TODO support non-contiguous memory
        if(!is_contiguous)
            throw mraft::RaftException(911, "apply provided non-contiguous memory");
        struct raft_buffer buf;
        buf.base = buf_info.ptr;
        buf.len  = buf_info.size * buf_info.itemsize;
        m_raft.apply(&buf, 1);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void barrier() {
        m_raft.barrier();
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void add(raft_id id, const std::string& address) {
        m_raft.add(id, address.c_str());
    }

    void assign(raft_id id, mraft::Role role) {
        m_raft.assign(id, role);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void remove(raft_id id) {
        m_raft.remove(id);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    void transfer(raft_id id) {
        m_raft.transfer(id);
        CHECK_FOR_PYTHON_EXCEPTION;
    }

    raft_id get_raft_id(const std::string& address) {
        auto id = m_raft.get_raft_id(address.c_str());
        CHECK_FOR_PYTHON_EXCEPTION;
        return id;
    }
};

PYBIND11_MODULE(pymraft, m) {
    m.doc() = "Python binding for the mochi-raft library";
    py::register_exception<mraft::RaftException>(m, "RaftException");
    py::enum_<mraft::Role>(m, "Role")
        .value("StandBy", mraft::Role::STANDBY)
        .value("Voter", mraft::Role::VOTER)
        .value("Spare", mraft::Role::SPARE)
        .export_values();
    py::class_<mraft::ServerInfo>(m, "ServerInfo")
        .def(py::init<raft_id, std::string, mraft::Role>())
        .def_readwrite("id", &mraft::ServerInfo::id)
        .def_readwrite("address", &mraft::ServerInfo::address)
        .def_readwrite("role", &mraft::ServerInfo::role);
    py::class_<PyRaft>(m, "Raft")
        .def(py::init<py_margo_instance_id, raft_id, py::object, uint16_t>(),
             "mid"_a, "id"_a, "fsm"_a, "provider_id"_a=0)
        .def("bootstrap", &PyRaft::bootstrap,
             "servers"_a)
        .def("recover", &PyRaft::recover,
             "servers"_a)
        .def("apply", &PyRaft::apply,
             "command"_a)
        .def("barrier", &PyRaft::barrier)
        .def("add", &PyRaft::add,
             "id"_a, "address"_a)
        .def("assign", &PyRaft::assign,
             "id"_a, "role"_a)
        .def("remove", &PyRaft::remove,
             "id"_a)
        .def("transfer", &PyRaft::transfer,
             "id"_a)
        .def("get_raft_id", &PyRaft::get_raft_id,
             "address"_a);
}
