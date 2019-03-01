extern crate gcc;

fn main() {
    gcc::Config::new().file("csrc/devdax.c").flag_if_supported("-march=native").compile("libdevdax.a");
}
