use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use wasmtime::*;
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};

// Tell rustfmt to skip this module reference; otherwise it can't seem to find it (`cargo fmt` says
// ".../wasmtime/benches does not exist".
#[rustfmt::skip]
mod common;

struct Server {
    permits: tokio::sync::Semaphore,
    engine: Engine,
    modules: Vec<Module>,
    instance_pres: Vec<InstancePre<WasiCtx>>,
}

impl Server {
    async fn job(self: Arc<Self>, index: usize) {
        let _permit = self.permits.acquire().await.unwrap();
        let ipre = &self.instance_pres[index % self.modules.len()];
        let wasi = WasiCtxBuilder::new().build();
        let mut store = Store::new(&self.engine, wasi);
        let instance = ipre.instantiate_async(&mut store).await.unwrap();
        //        let start_func = instance.get_func(&mut store, "_start").unwrap();
        //        start_func
        //            .call_async(&mut store, &[], &mut [])
        //            .await
        //            .unwrap();
    }
}

fn run_server(
    c: &mut Criterion,
    strategy: &InstanceAllocationStrategy,
    filenames: &[&str],
    occupancy: usize,
) {
    let engine = common::make_engine(strategy, /* async = */ true).unwrap();
    let mut instance_pres = vec![];
    let mut modules = vec![];
    for filename in filenames {
        let (module, linker) = common::load_module(&engine, filename).unwrap();
        let instance_pre = common::instantiate_pre(&linker, &module).unwrap();
        modules.push(module);
        instance_pres.push(instance_pre);
    }

    let server = Arc::new(Server {
        permits: tokio::sync::Semaphore::new(occupancy),
        engine,
        modules,
        instance_pres,
    });

    c.bench_function(
        &format!(
            "strategy {}, occupancy {}, benches {:?}",
            common::benchmark_name(strategy),
            occupancy,
            filenames
        ),
        move |b| {
            let server_clone = server.clone();
            b.iter_custom(move |instantiations| {
                let server_clone = server_clone.clone();
                let rt = tokio::runtime::Runtime::new().unwrap();
                let now = std::time::Instant::now();
                rt.block_on(async move {
                    for i in 0..instantiations {
                        let server = server_clone.clone();
                        tokio::spawn(server.job(i as usize));
                    }
                });
                now.elapsed()
            });
        },
    );
}

fn bench_server(c: &mut Criterion) {
    common::build_wasi_example();

    //    let modules = vec!["wasi.wasm"];
    let modules = vec!["spidermonkey.wasm"];
    let occupancy = 1000;

    for strategy in common::strategies() {
        run_server(c, &strategy, &modules[..], occupancy);
    }
}

criterion_group!(benches, bench_server);
criterion_main!(benches);
