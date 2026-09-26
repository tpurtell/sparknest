//! Raw verbs on this machine's hardware: two RC QPs on one device connected
//! to each other. Skips when no RoCE rail is present.

use nest_fabric::verbs::{Context, Cq, Qp, QpInfo, Region};
use nest_fabric::{Completion, OP_RECV, OP_RECV_IMM, OP_SEND, OP_WRITE};
use std::time::{Duration, Instant};

fn wait(cq: &Cq, want: usize) -> Vec<Completion> {
    let mut got = Vec::new();
    let mut buf = [Completion::default(); 16];
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < want {
        let n = cq.poll(&mut buf).unwrap();
        got.extend_from_slice(&buf[..n]);
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions: {got:?}"
        );
    }
    got
}

#[test]
fn write_with_imm_and_send_over_loopback() {
    let rails = nest_fabric::discover(&[]);
    eprintln!("rails: {rails:#?}");
    let Some(rail) = rails.first() else { return };
    let ctx = Context::open(&rail.ibdev).unwrap();
    let (_state, mtu) = ctx.port(rail.port).unwrap();
    let cq_a = Cq::new(&ctx, 64, false).unwrap();
    let cq_b = Cq::new(&ctx, 64, false).unwrap();
    let a = Qp::new(&ctx, &cq_a, &cq_a, 16, 16, rail.port).unwrap();
    let b = Qp::new(&ctx, &cq_b, &cq_b, 16, 16, rail.port).unwrap();
    let ia = QpInfo {
        qpn: a.num(),
        psn: 1,
        gid: rail.gid,
        mtu,
    };
    let ib = QpInfo {
        qpn: b.num(),
        psn: 2,
        gid: rail.gid,
        mtu,
    };
    a.connect(rail.gid_index, ia.psn, &ib, mtu).unwrap();
    b.connect(rail.gid_index, ib.psn, &ia, mtu).unwrap();

    let src = Region::new(&ctx, 4 << 20, false).unwrap();
    let dst = Region::new(&ctx, 4 << 20, true).unwrap();
    let msg = Region::new(&ctx, 4096, false).unwrap();
    unsafe {
        for (i, b) in src.slice_mut(0, 4 << 20).iter_mut().enumerate() {
            *b = (i % 253) as u8;
        }
        // B posts two receives: one consumed by write-with-imm, one by send.
        b.post_recv(1, std::ptr::null_mut(), 0, 0).unwrap();
        b.post_recv(2, msg.ptr_at(0), 256, msg.lkey()).unwrap();
        a.post_write_imm(
            10,
            src.ptr_at(0),
            4 << 20,
            src.lkey(),
            dst.addr(),
            dst.rkey(),
            0xabc,
        )
        .unwrap();
        let hello = b"request";
        src.slice_mut(0, hello.len()).copy_from_slice(hello);
    }
    let wa = wait(&cq_a, 1);
    assert_eq!((wa[0].status, wa[0].opcode), (0, OP_WRITE));
    let wb = wait(&cq_b, 1);
    assert_eq!(
        (wb[0].status, wb[0].opcode, wb[0].imm, wb[0].has_imm),
        (0, OP_RECV_IMM, 0xabc, 1)
    );
    unsafe {
        assert!(
            dst.slice(7, (4 << 20) - 7)
                .iter()
                .enumerate()
                .all(|(i, b)| *b == ((i + 7) % 253) as u8)
        );
        a.post_send(11, src.ptr_at(0), 7, src.lkey(), true).unwrap();
    }
    assert_eq!(wait(&cq_a, 1)[0].opcode, OP_SEND);
    let wb = wait(&cq_b, 1);
    assert_eq!((wb[0].opcode, wb[0].byte_len, wb[0].wr_id), (OP_RECV, 7, 2));
    assert_eq!(unsafe { msg.slice(0, 7) }, b"request");
}
