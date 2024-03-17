fn main() {
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rustc-link-lib=static=adbc_driver_common");
    println!("cargo:rustc-link-lib=static=adbc_driver_sqlite");
    println!("cargo:rustc-link-lib=static=nanoarrow");
    println!("cargo:rustc-link-lib=static=sqlite3");
}
