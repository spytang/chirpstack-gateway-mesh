use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{OnceCell, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RouteEntry {
    pub path_cost: u32,
    pub depth: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NeighborMetrics {
    pub path_cost: u32,
    pub depth: u16,
}

#[derive(Debug, Default)]
pub struct TrickleRouteSelector {
    local_route: RwLock<Option<RouteEntry>>,
    neighbors: RwLock<HashMap<[u8; 4], NeighborMetrics>>,
}

impl TrickleRouteSelector {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_local_route(&self, entry: Option<RouteEntry>) {
        let mut guard = self.local_route.write().await;
        *guard = entry;
    }

    pub async fn local_route(&self) -> Option<RouteEntry> {
        *self.local_route.read().await
    }

    pub async fn on_beacon(&self, relay_id: [u8; 4], path_cost: u32, depth: u16) {
        let mut guard = self.neighbors.write().await;
        guard.insert(relay_id, NeighborMetrics { path_cost, depth });
    }

    pub async fn neighbor_metrics(&self, relay_id: &[u8; 4]) -> Option<NeighborMetrics> {
        self.neighbors.read().await.get(relay_id).copied()
    }

    pub async fn reset(&self) {
        self.local_route.write().await.take();
        self.neighbors.write().await.clear();
    }
}

static ROUTE_SELECTOR: OnceCell<Arc<TrickleRouteSelector>> = OnceCell::const_new();

pub async fn selector() -> Arc<TrickleRouteSelector> {
    ROUTE_SELECTOR
        .get_or_init(|| async { Arc::new(TrickleRouteSelector::default()) })
        .await
        .clone()
}

pub async fn reset() {
    if let Some(selector) = ROUTE_SELECTOR.get() {
        selector.reset().await;
    }
}
