//! systemd notifications (`sd_notify`), without a dependency: readiness and
//! a one-line status shown by `systemctl status`.

/// Send `msg` (e.g. `READY=1`, `STATUS=...`) if systemd asked for it.
pub fn notify(msg: &str) {
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else {
        return;
    };
    let bytes = path.as_encoded_bytes();
    let r = if let Some(name) = bytes.strip_prefix(b"@") {
        use std::os::linux::net::SocketAddrExt;
        std::os::unix::net::SocketAddr::from_abstract_name(name)
            .and_then(|a| sock.send_to_addr(msg.as_bytes(), &a))
    } else {
        sock.send_to(msg.as_bytes(), &path)
    };
    if let Err(e) = r {
        tracing::debug!(error = %e, "sd_notify failed");
    }
}

/// The status line `systemctl status` shows.
pub fn status(s: &str) {
    tracing::info!(status = s);
    notify(&format!("STATUS={s}"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn delivers_to_the_notify_socket() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("notify");
        let rx = std::os::unix::net::UnixDatagram::bind(&p).unwrap();
        // SAFETY: this test binary does not read the environment concurrently.
        unsafe { std::env::set_var("NOTIFY_SOCKET", &p) };
        super::notify("READY=1");
        let mut buf = [0u8; 64];
        let n = rx.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"READY=1");
    }
}
