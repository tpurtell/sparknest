/*
 * Minimal libibverbs shim for sparknest's fabric.
 *
 * Exposes a tiny, stable C ABI of our own design so the Rust side needs no
 * bindgen and does not depend on rdma-core header versions (the Sparks run
 * rdma-core 50, raptor 61). Several verbs entry points are static inline in
 * <infiniband/verbs.h>; wrapping them here is the reason this file exists.
 *
 * Every function returns 0 / non-NULL on success; on failure errno is set.
 */
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>

struct nf_ctx {
	struct ibv_context *ctx;
	struct ibv_pd *pd;
};

struct nf_cq {
	struct ibv_cq *cq;
	struct ibv_comp_channel *ch;
};

struct nf_mr {
	struct ibv_mr *mr;
};

struct nf_qp {
	struct ibv_qp *qp;
};

/* Completion as seen by Rust. */
typedef struct {
	uint64_t wr_id;
	uint32_t status;   /* 0 = success, else ibv_wc_status */
	uint32_t opcode;   /* NF_OP_* */
	uint32_t byte_len;
	uint32_t imm;
	uint32_t has_imm;
	uint32_t qp_num;
} nf_wc;

enum { NF_OP_OTHER = 0, NF_OP_SEND = 1, NF_OP_RECV = 2, NF_OP_WRITE = 3, NF_OP_RECV_IMM = 4 };

struct nf_ctx *nf_open(const char *ibdev)
{
	struct ibv_device **list;
	struct nf_ctx *c;
	int n, i;

	list = ibv_get_device_list(&n);
	if (!list)
		return NULL;
	c = calloc(1, sizeof *c);
	for (i = 0; c && i < n; i++) {
		if (!strcmp(ibv_get_device_name(list[i]), ibdev)) {
			c->ctx = ibv_open_device(list[i]);
			break;
		}
	}
	ibv_free_device_list(list);
	if (!c)
		return NULL;
	if (!c->ctx) {
		free(c);
		errno = ENODEV;
		return NULL;
	}
	c->pd = ibv_alloc_pd(c->ctx);
	if (!c->pd) {
		ibv_close_device(c->ctx);
		free(c);
		return NULL;
	}
	return c;
}

void nf_close(struct nf_ctx *c)
{
	if (!c)
		return;
	ibv_dealloc_pd(c->pd);
	ibv_close_device(c->ctx);
	free(c);
}

int nf_query_port(struct nf_ctx *c, uint8_t port, uint32_t *state, uint32_t *active_mtu)
{
	struct ibv_port_attr a;
	if (ibv_query_port(c->ctx, port, &a))
		return -1;
	*state = a.state;
	*active_mtu = a.active_mtu;
	return 0;
}

int nf_max_qp_wr(struct nf_ctx *c)
{
	struct ibv_device_attr a;
	if (ibv_query_device(c->ctx, &a))
		return -1;
	return a.max_qp_wr;
}

struct nf_mr *nf_reg(struct nf_ctx *c, void *addr, size_t len, int remote_write)
{
	struct nf_mr *m = calloc(1, sizeof *m);
	int access = IBV_ACCESS_LOCAL_WRITE;
	if (!m)
		return NULL;
	if (remote_write)
		access |= IBV_ACCESS_REMOTE_WRITE;
	m->mr = ibv_reg_mr(c->pd, addr, len, access);
	if (!m->mr) {
		free(m);
		return NULL;
	}
	return m;
}

uint32_t nf_lkey(const struct nf_mr *m) { return m->mr->lkey; }
uint32_t nf_rkey(const struct nf_mr *m) { return m->mr->rkey; }

void nf_dereg(struct nf_mr *m)
{
	if (!m)
		return;
	ibv_dereg_mr(m->mr);
	free(m);
}

struct nf_cq *nf_cq_create(struct nf_ctx *c, int depth, int with_channel)
{
	struct nf_cq *q = calloc(1, sizeof *q);
	if (!q)
		return NULL;
	if (with_channel) {
		q->ch = ibv_create_comp_channel(c->ctx);
		if (!q->ch) {
			free(q);
			return NULL;
		}
	}
	q->cq = ibv_create_cq(c->ctx, depth, NULL, q->ch, 0);
	if (!q->cq) {
		if (q->ch)
			ibv_destroy_comp_channel(q->ch);
		free(q);
		return NULL;
	}
	return q;
}

int nf_cq_fd(const struct nf_cq *q) { return q->ch ? q->ch->fd : -1; }

int nf_cq_arm(struct nf_cq *q) { return ibv_req_notify_cq(q->cq, 0); }

/* Consume one completion-channel event (after the fd became readable). */
int nf_cq_take_event(struct nf_cq *q)
{
	struct ibv_cq *cq;
	void *ctx;
	if (ibv_get_cq_event(q->ch, &cq, &ctx))
		return -1;
	ibv_ack_cq_events(cq, 1);
	return 0;
}

void nf_cq_destroy(struct nf_cq *q)
{
	if (!q)
		return;
	ibv_destroy_cq(q->cq);
	if (q->ch)
		ibv_destroy_comp_channel(q->ch);
	free(q);
}

int nf_poll(struct nf_cq *q, nf_wc *out, int max)
{
	struct ibv_wc wc[32];
	int n, i;
	if (max > 32)
		max = 32;
	n = ibv_poll_cq(q->cq, max, wc);
	for (i = 0; i < n; i++) {
		out[i].wr_id = wc[i].wr_id;
		out[i].status = wc[i].status;
		out[i].byte_len = wc[i].byte_len;
		out[i].qp_num = wc[i].qp_num;
		out[i].has_imm = (wc[i].wc_flags & IBV_WC_WITH_IMM) ? 1 : 0;
		out[i].imm = out[i].has_imm ? ntohl(wc[i].imm_data) : 0;
		switch (wc[i].opcode) {
		case IBV_WC_SEND: out[i].opcode = NF_OP_SEND; break;
		case IBV_WC_RECV: out[i].opcode = NF_OP_RECV; break;
		case IBV_WC_RDMA_WRITE: out[i].opcode = NF_OP_WRITE; break;
		case IBV_WC_RECV_RDMA_WITH_IMM: out[i].opcode = NF_OP_RECV_IMM; break;
		default: out[i].opcode = NF_OP_OTHER; break;
		}
	}
	return n;
}

struct nf_qp *nf_qp_create(struct nf_ctx *c, struct nf_cq *scq, struct nf_cq *rcq,
			   uint32_t max_send, uint32_t max_recv, uint8_t port)
{
	struct ibv_qp_init_attr init;
	struct ibv_qp_attr a;
	struct nf_qp *q = calloc(1, sizeof *q);
	if (!q)
		return NULL;
	memset(&init, 0, sizeof init);
	init.send_cq = scq->cq;
	init.recv_cq = rcq->cq;
	init.qp_type = IBV_QPT_RC;
	init.cap.max_send_wr = max_send;
	init.cap.max_recv_wr = max_recv;
	init.cap.max_send_sge = 1;
	init.cap.max_recv_sge = 1;
	init.cap.max_inline_data = 64;
	q->qp = ibv_create_qp(c->pd, &init);
	if (!q->qp) {
		free(q);
		return NULL;
	}
	memset(&a, 0, sizeof a);
	a.qp_state = IBV_QPS_INIT;
	a.pkey_index = 0;
	a.port_num = port;
	a.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
	if (ibv_modify_qp(q->qp, &a, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT
			  | IBV_QP_ACCESS_FLAGS)) {
		int e = errno;
		ibv_destroy_qp(q->qp);
		free(q);
		errno = e;
		return NULL;
	}
	return q;
}

uint32_t nf_qp_num(const struct nf_qp *q) { return q->qp->qp_num; }

int nf_qp_connect(struct nf_qp *q, uint8_t port, int sgid_index, uint32_t local_psn,
		  const uint8_t remote_gid[16], uint32_t remote_qpn, uint32_t remote_psn,
		  uint32_t mtu)
{
	struct ibv_qp_attr a;
	memset(&a, 0, sizeof a);
	a.qp_state = IBV_QPS_RTR;
	a.path_mtu = (enum ibv_mtu)mtu;
	a.dest_qp_num = remote_qpn;
	a.rq_psn = remote_psn;
	a.max_dest_rd_atomic = 1;
	a.min_rnr_timer = 12;
	a.ah_attr.is_global = 1;
	a.ah_attr.port_num = port;
	memcpy(a.ah_attr.grh.dgid.raw, remote_gid, 16);
	a.ah_attr.grh.sgid_index = (uint8_t)sgid_index;
	a.ah_attr.grh.hop_limit = 64;
	if (ibv_modify_qp(q->qp, &a, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU
			  | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC
			  | IBV_QP_MIN_RNR_TIMER))
		return -1;
	memset(&a, 0, sizeof a);
	a.qp_state = IBV_QPS_RTS;
	/* 4.096 us x 2^12 = 17 ms before resending a lost packet (14, the
	 * usual default, is 67 ms: every drop under incast cost that much). */
	a.timeout = 12;
	a.retry_cnt = 7;
	a.rnr_retry = 7;
	a.sq_psn = local_psn;
	a.max_rd_atomic = 1;
	return ibv_modify_qp(q->qp, &a, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT
			     | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}

/* Move a QP to the error state: outstanding work completes with flush errors. */
int nf_qp_error(struct nf_qp *q)
{
	struct ibv_qp_attr a;
	memset(&a, 0, sizeof a);
	a.qp_state = IBV_QPS_ERR;
	return ibv_modify_qp(q->qp, &a, IBV_QP_STATE);
}

void nf_qp_destroy(struct nf_qp *q)
{
	if (!q)
		return;
	ibv_destroy_qp(q->qp);
	free(q);
}

int nf_post_recv(struct nf_qp *q, uint64_t wr_id, void *addr, uint32_t len, uint32_t lkey)
{
	struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
	struct ibv_recv_wr wr = { .wr_id = wr_id, .sg_list = &sge, .num_sge = len ? 1 : 0 };
	struct ibv_recv_wr *bad;
	return ibv_post_recv(q->qp, &wr, &bad);
}

int nf_post_send(struct nf_qp *q, uint64_t wr_id, void *addr, uint32_t len, uint32_t lkey,
		 int inline_data)
{
	struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
	struct ibv_send_wr wr, *bad;
	memset(&wr, 0, sizeof wr);
	wr.wr_id = wr_id;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED | (inline_data ? IBV_SEND_INLINE : 0);
	return ibv_post_send(q->qp, &wr, &bad);
}

int nf_post_write_imm(struct nf_qp *q, uint64_t wr_id, void *addr, uint32_t len,
		      uint32_t lkey, uint64_t raddr, uint32_t rkey, uint32_t imm)
{
	struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
	struct ibv_send_wr wr, *bad;
	memset(&wr, 0, sizeof wr);
	wr.wr_id = wr_id;
	wr.sg_list = len ? &sge : NULL;
	wr.num_sge = len ? 1 : 0;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = htonl(imm);
	wr.wr.rdma.remote_addr = raddr;
	wr.wr.rdma.rkey = rkey;
	return ibv_post_send(q->qp, &wr, &bad);
}
