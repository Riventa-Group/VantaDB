pub mod proto {
    tonic::include_proto!("vantadb");
}

mod client;
pub use client::VantaClient;
