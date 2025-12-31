use wasmtime::component::bindgen;

bindgen!({
    world: "source",
    path: "../../assets/wit",
    exports: { default: async },
    imports: {
        "tangent:logs/remote.call-batch": async,
    },
});
