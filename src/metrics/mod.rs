use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    pub requests_total: IntCounterVec,
    pub request_latency_seconds: HistogramVec,
    pub bytes_in_total: IntCounter,
    pub bytes_out_total: IntCounter,
    pub cache_hits_total: IntCounter,
    pub cache_misses_total: IntCounter,
    pub storage_compactions_total: IntCounter,
    pub replication_failures_total: IntCounter,
    pub multipart_uploads_active: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new("fasterstore_requests_total", "Total HTTP requests"),
            &["method", "route", "status"],
        )
        .expect("requests_total counter");

        let request_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "fasterstore_request_latency_seconds",
                "Request latency by method and route",
            ),
            &["method", "route"],
        )
        .expect("request_latency histogram");

        let bytes_in_total = IntCounter::new("fasterstore_bytes_in_total", "Total incoming bytes")
            .expect("bytes_in");
        let bytes_out_total =
            IntCounter::new("fasterstore_bytes_out_total", "Total outgoing bytes")
                .expect("bytes_out");

        let cache_hits_total =
            IntCounter::new("fasterstore_cache_hits_total", "In-memory cache hits")
                .expect("cache_hits");
        let cache_misses_total =
            IntCounter::new("fasterstore_cache_misses_total", "In-memory cache misses")
                .expect("cache_misses");

        let storage_compactions_total = IntCounter::new(
            "fasterstore_storage_compactions_total",
            "Number of completed storage compaction runs",
        )
        .expect("storage_compactions");

        let replication_failures_total = IntCounter::new(
            "fasterstore_replication_failures_total",
            "Replication failures",
        )
        .expect("replication_failures");

        let multipart_uploads_active = IntGauge::new(
            "fasterstore_multipart_uploads_active",
            "Active multipart upload sessions",
        )
        .expect("multipart gauge");

        registry
            .register(Box::new(requests_total.clone()))
            .expect("register requests_total");
        registry
            .register(Box::new(request_latency_seconds.clone()))
            .expect("register request_latency_seconds");
        registry
            .register(Box::new(bytes_in_total.clone()))
            .expect("register bytes_in_total");
        registry
            .register(Box::new(bytes_out_total.clone()))
            .expect("register bytes_out_total");
        registry
            .register(Box::new(cache_hits_total.clone()))
            .expect("register cache_hits_total");
        registry
            .register(Box::new(cache_misses_total.clone()))
            .expect("register cache_misses_total");
        registry
            .register(Box::new(storage_compactions_total.clone()))
            .expect("register storage_compactions_total");
        registry
            .register(Box::new(replication_failures_total.clone()))
            .expect("register replication_failures_total");
        registry
            .register(Box::new(multipart_uploads_active.clone()))
            .expect("register multipart_uploads_active");

        Self {
            registry,
            requests_total,
            request_latency_seconds,
            bytes_in_total,
            bytes_out_total,
            cache_hits_total,
            cache_misses_total,
            storage_compactions_total,
            replication_failures_total,
            multipart_uploads_active,
        }
    }

    pub fn encode(&self) -> Result<String, String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| e.to_string())?;
        String::from_utf8(buffer).map_err(|e| e.to_string())
    }
}
