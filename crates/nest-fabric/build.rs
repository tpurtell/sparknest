fn main() {
    println!("cargo:rerun-if-changed=csrc/nf_shim.c");
    cc::Build::new()
        .file("csrc/nf_shim.c")
        .flag_if_supported("-Wall")
        .flag_if_supported("-Wextra")
        .opt_level(2)
        .compile("nf_shim");
    println!("cargo:rustc-link-lib=ibverbs");
}
