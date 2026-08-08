#ifndef RDMA_COMMUNICATION_H
#define RDMA_COMMUNICATION_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>

#ifdef HAS_RDMA
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#endif

#define RDMA_BUFFER_SIZE (64 * 1024)
#define RDMA_CQ_SIZE 1024
#define RDMA_MAX_WR 512
#define RDMA_CACHELINE_SIZE 64

enum RDMAOpType { RDMA_OP_READ = 0, RDMA_OP_WRITE = 1, RDMA_OP_READ_RESP = 2, RDMA_OP_WRITE_RESP = 3 };

struct RDMARequest {
    uint8_t op_type;
    uint64_t addr;
    uint64_t size;
    uint64_t timestamp;
    uint8_t host_id;
    uint64_t virtual_addr;
    uint8_t data[RDMA_BUFFER_SIZE];
} __attribute__((packed));

struct RDMAResponse {
    uint8_t status;
    uint64_t latency_ns;
    uint8_t cache_state;
    uint8_t data[RDMA_BUFFER_SIZE];
} __attribute__((packed));

struct RDMAMessage {
    RDMARequest request;
    RDMAResponse response;
} __attribute__((packed));

class RDMAConnection {
public:
#ifdef HAS_RDMA
    struct ConnectionInfo {
        struct ibv_context *context;
        struct ibv_pd *pd;
        struct ibv_mr *mr;
        struct ibv_cq *send_cq;
        struct ibv_cq *recv_cq;
        struct ibv_qp *qp;
        struct ibv_comp_channel *comp_channel;
        void *buffer;
        size_t buffer_size;
        std::atomic<bool> connected;
    };
#endif

    using MessageHandler = std::function<void(const RDMAMessage &, RDMAMessage &)>;

protected:
#ifdef HAS_RDMA
    ConnectionInfo conn_info_;
    struct rdma_cm_id *cm_id_;
    struct rdma_event_channel *event_channel_;
#endif
    MessageHandler message_handler_;
    std::atomic<bool> running_;
    std::atomic<bool> connected_;

#ifdef HAS_RDMA
    int setup_connection_resources();
    int setup_qp_parameters(struct ibv_qp_init_attr &qp_attr);
    int register_memory_region();
    int post_receive();
    int post_send(const RDMAMessage *msg);
    void cleanup_resources();
#endif

public:
    RDMAConnection();
    virtual ~RDMAConnection();

#ifdef HAS_RDMA
    int accept_cm_id(struct rdma_cm_id *id);
#endif
    void mark_connected();
    void set_message_handler(MessageHandler handler) { message_handler_ = handler; }
    int send_message(const RDMAMessage &msg);
    int receive_message(RDMAMessage &msg, int timeout_ms = -1);
    bool is_connected() const { return connected_.load(); }
    void disconnect();
};

class RDMAServer : public RDMAConnection {
private:
    std::string bind_addr_;
    uint16_t port_;
#ifdef HAS_RDMA
    struct rdma_cm_id *listen_id_;
    std::map<struct rdma_cm_id *, std::shared_ptr<RDMAConnection>> pending_connections_;
#endif

public:
    RDMAServer(const std::string &addr, uint16_t port);
    ~RDMAServer();

    int start();
    std::shared_ptr<RDMAConnection> accept_connection();
    void handle_client(const std::shared_ptr<RDMAConnection> &client);
    void stop();
};

class RDMAClient : public RDMAConnection {
private:
    std::string server_addr_;
    uint16_t server_port_;

public:
    RDMAClient(const std::string &addr, uint16_t port);
    ~RDMAClient();

    int connect();
    int send_request(const RDMARequest &req, RDMAResponse &resp);
};

class RDMATransport {
public:
    enum Mode { MODE_TCP, MODE_SHM, MODE_RDMA };

    static Mode get_transport_mode() {
        const char *mode = std::getenv("CXL_TRANSPORT_MODE");
        if (mode) {
            if (std::string(mode) == "rdma")
                return MODE_RDMA;
            if (std::string(mode) == "shm")
                return MODE_SHM;
            if (std::string(mode) == "tcp")
                return MODE_TCP;
        }
        return MODE_TCP;
    }

    static bool is_rdma_available() {
#ifdef HAS_RDMA
        struct ibv_device **dev_list = ibv_get_device_list(nullptr);
        if (dev_list) {
            ibv_free_device_list(dev_list);
            return true;
        }
#endif
        return false;
    }
};

#endif // RDMA_COMMUNICATION_H
