use pylon_load::ceiling::spec;

fn main() {
    let spec = spec::detect();
    println!("{:#?}", spec);
}
