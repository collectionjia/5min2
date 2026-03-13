use std::time::Duration;

pub struct CheckConfig {
    timeout: Duration,
}

pub struct CheckConfigBuilder {
    timeout: Option<Duration>,
}

impl CheckConfig {
    pub fn builder() -> CheckConfigBuilder {
        CheckConfigBuilder { timeout: None }
    }
}

impl CheckConfigBuilder {
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }
    pub fn build(self) -> CheckConfig {
        CheckConfig {
            timeout: self.timeout.unwrap_or(Duration::from_secs(5)),
        }
    }
}

#[derive(Clone, Copy)]
pub enum RpcStatus {
    Healthy,
    Unreachable,
}

pub struct RpcChecker {
    _cfg: CheckConfig,
}

impl RpcChecker {
    pub fn new(cfg: CheckConfig) -> Self {
        RpcChecker { _cfg: cfg }
    }
    pub fn validate_endpoint(&self, _url: &str) -> bool {
        true
    }
    pub fn perform_health_check_sync(&self, _url: &str) -> RpcStatus {
        RpcStatus::Healthy
    }
}

pub struct CircuitBreaker {
    open: bool,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        CircuitBreaker { open: false }
    }
    pub fn status_allows_request(&self, _status: RpcStatus) -> bool {
        true
    }
    pub fn is_open(&self) -> bool {
        self.open
    }
    pub fn record_success(&self) {}
}

pub struct Metrics;

impl Metrics {
    pub fn new() -> Self {
        Metrics
    }
    pub fn record_check(&self, _ok: bool) {}
}

pub struct EndpointValidator;

impl EndpointValidator {
    pub fn new() -> Self {
        EndpointValidator
    }
    pub fn normalize(&self, url: &str) -> String {
        url.to_string()
    }
}
