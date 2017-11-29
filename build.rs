extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("schema/messages.capnp")
        .run()
        .expect("Failed compiling messages schema");
}
