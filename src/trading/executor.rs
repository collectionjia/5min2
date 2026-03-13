use anyhow::Result;
use alloy::signers::Signer;
use alloy::signers::local::LocalSigner;
use chrono::Utc;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::types::{Address, Decimal, U256};
use polymarket_client_sdk::POLYGON;
use rust_decimal_macros::dec;
use std::str::FromStr;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::monitor::arbitrage::ArbitrageOpportunity;

pub struct OrderPairResult {
    pub pair_id: String,
    pub yes_order_id: String,
    pub no_order_id: String,
    pub yes_filled: Decimal,
    pub no_filled: Decimal,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub success: bool,
}

pub struct TradingExecutor {
    client: Option<Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
    private_key: String,
    max_order_size: Decimal,
    slippage: [Decimal; 2], // [first, second], down side uses second, up/flat uses first
    gtd_expiration_secs: u64,
    arbitrage_order_type: OrderType,
    dry_run: bool,
}

impl TradingExecutor {
    pub async fn new(
        private_key: String,
        max_order_size_usdc: f64,
        proxy_address: Option<Address>,
        slippage: [f64; 2],
        gtd_expiration_secs: u64,
        arbitrage_order_type: OrderType,
        dry_run: bool,
    ) -> Result<Self> {
        if dry_run {
            info!("[模拟模式] 交易执行器已启动（不进行真实交易）");
            return Ok(Self {
                client: None,
                private_key,
                max_order_size: Decimal::try_from(max_order_size_usdc)
                    .unwrap_or(rust_decimal_macros::dec!(100.0)),
                slippage: [
                    Decimal::try_from(slippage[0]).unwrap_or(dec!(0.0)),
                    Decimal::try_from(slippage[1]).unwrap_or(dec!(0.01)),
                ],
                gtd_expiration_secs,
                arbitrage_order_type,
                dry_run,
            });
        }

        // Validate private key format
        let signer = LocalSigner::from_str(&private_key)
            .map_err(|e| anyhow::anyhow!("invalid private key format: {}. Ensure the key is a 64-char hex string (without 0x prefix)", e))?
            .with_chain_id(Some(POLYGON));

        let config = Config::builder().use_server_time(false).build();
        let mut auth_builder = Client::new("https://clob.polymarket.com", config)
            .map_err(|e| anyhow::anyhow!("failed to create CLOB client: {}", e))?
            .authentication_builder(&signer);

        // If proxy_address provided, set funder and signature_type (following Python SDK pattern)
        if let Some(funder) = proxy_address {
            auth_builder = auth_builder
                .funder(funder)
                .signature_type(SignatureType::Proxy);
        }

        let client = auth_builder
            .authenticate()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "API authentication failed: {}. Possible causes: 1) invalid private key 2) network issue 3) Polymarket API unavailable",
                    e
                )
            })?;

        Ok(Self {
            client: Some(client),
            private_key,
            max_order_size: Decimal::try_from(max_order_size_usdc)
                .unwrap_or(rust_decimal_macros::dec!(100.0)),
            slippage: [
                Decimal::try_from(slippage[0]).unwrap_or(dec!(0.0)),
                Decimal::try_from(slippage[1]).unwrap_or(dec!(0.01)),
            ],
            gtd_expiration_secs,
            arbitrage_order_type,
            dry_run,
        })
    }

    /// Verify authentication succeeded - using api_keys() per official example
    pub async fn verify_authentication(&self) -> Result<()> {
        if self.dry_run {
            info!("[模拟模式] 跳过鉴权验证");
            return Ok(());
        }
        // Per official example, use api_keys() to verify authentication status
        self.client.as_ref().unwrap().api_keys().await
            .map_err(|e| anyhow::anyhow!("authentication verification failed: API call returned error: {}", e))?;
        Ok(())
    }

    /// Cancel all pending orders for this account (used during wind-down)
    pub async fn cancel_all_orders(&self) -> Result<()> {
        if self.dry_run {
            info!("[模拟模式] 模拟取消所有未完成订单");
            return Ok(());
        }
        self.client.as_ref().unwrap()
            .cancel_all_orders()
            .await
            .map_err(|e| anyhow::anyhow!("failed to cancel all pending orders: {}", e))?;
        info!("✅ 已取消所有未完成订单");
        Ok(())
    }

    /// Place GTC sell order at specified price (market-intent sell for single-leg position during wind-down)
    pub async fn sell_at_price(
        &self,
        token_id: U256,
        price: Decimal,
        size: Decimal,
    ) -> Result<()> {
        if self.dry_run {
            info!(
                "[模拟模式] 模拟卖出 | token_id={:#x} | 价格:{:.4} | 数量:{}",
                token_id, price, size
            );
            return Ok(());
        }
        let client = self.client.as_ref().unwrap();
        let signer = LocalSigner::from_str(&self.private_key)?
            .with_chain_id(Some(POLYGON));
        let order = client
            .limit_order()
            .token_id(token_id)
            .side(Side::Sell)
            .price(price)
            .size(size)
            .order_type(OrderType::GTC)
            .build()
            .await?;
        let signed = client.sign(&signer, order).await?;
        match client.post_order(signed).await {
            Ok(_) => {
                info!(
                    "✅ 卖单提交成功 | token_id={:#x} | price:{:.4} | size:{}",
                    token_id, price, size
                );
            }
            Err(e) => {
                error!(
                    "❌ 卖单提交失败 | token_id={:#x} | price:{:.4} | size:{} | error:{}",
                    token_id, price, size, e
                );
                return Err(anyhow::anyhow!("sell order submission failed: {}", e));
            }
        }
        Ok(())
    }

    /// Get slippage by direction: down(↓) uses second, up(↑) and flat use first
    fn slippage_for_direction(&self, dir: &str) -> Decimal {
        if dir == "↓" {
            self.slippage[1]
        } else {
            self.slippage[0]
        }
    }

    /// Execute arbitrage trade: illiquid side first (FOK), then liquid side (GTD).
    /// 不流动侧 = 可用数量更少的一侧。FOK 不成交则跳过（不产生损失）。
    /// yes_dir / no_dir: price direction "↑" "↓" "−" or "", used to assign slippage by direction (down=second, up/flat=first)
    pub async fn execute_arbitrage_pair(
        &self,
        opp: &ArbitrageOpportunity,
        yes_dir: &str,
        no_dir: &str,
    ) -> Result<OrderPairResult> {
        // Performance timing: total start
        let total_start = Instant::now();

        debug!(
            market_id = %opp.market_id,
            profit_pct = %opp.profit_percentage,
            "开始套利交易（不流动侧 FOK → 流动侧 GTD）"
        );

        // Calculate actual order size (considering max order limit)
        let yes_token_id = U256::from_str(&opp.yes_token_id.to_string())?;
        let no_token_id = U256::from_str(&opp.no_token_id.to_string())?;

        let order_size = opp.yes_size.min(opp.no_size).min(self.max_order_size);

        // Generate order pair ID
        let pair_id = Uuid::new_v4().to_string();

        // Slippage by direction: up=first, down/flat=second
        let yes_slippage_apply = self.slippage_for_direction(yes_dir);
        let no_slippage_apply = self.slippage_for_direction(no_dir);
        // 规范化价格到最小跳动 0.01（两位小数），避免“Price has 3 decimal places”错误
        let yes_price_with_slippage = (opp.yes_ask_price + yes_slippage_apply)
            .min(dec!(1.0))
            .round_dp(2);
        let no_price_with_slippage = (opp.no_ask_price + no_slippage_apply)
            .min(dec!(1.0))
            .round_dp(2);

        // Determine illiquid side: smaller available size = less liquidity
        let yes_is_illiquid = opp.yes_size <= opp.no_size;
        let (illiquid_label, liquid_label) = if yes_is_illiquid { ("YES", "NO") } else { ("NO", "YES") };
        let (illiquid_token, liquid_token) = if yes_is_illiquid {
            (yes_token_id, no_token_id)
        } else {
            (no_token_id, yes_token_id)
        };
        let (illiquid_price, liquid_price) = if yes_is_illiquid {
            (yes_price_with_slippage, no_price_with_slippage)
        } else {
            (no_price_with_slippage, yes_price_with_slippage)
        };
        let (illiquid_size, liquid_size) = if yes_is_illiquid {
            (opp.yes_size, opp.no_size)
        } else {
            (opp.no_size, opp.yes_size)
        };

        // Print level selection info (price with slippage)
        info!(
            "📋 盘口 | YES {:.2}×{:.2} NO {:.2}×{:.2} | 不流动侧:{} (可用:{})",
            yes_price_with_slippage, order_size,
            no_price_with_slippage, order_size,
            illiquid_label, illiquid_size
        );

        info!(
            "📤 下单 | {} FOK {:.2}×{} → {} GTD {:.2}×{} | 过期:{}秒",
            illiquid_label, illiquid_price, order_size,
            liquid_label, liquid_price, order_size,
            self.gtd_expiration_secs
        );

        // Pre-order check: both sides must be > $1 (exchange minimum)
        let yes_amount_usd = yes_price_with_slippage * order_size;
        let no_amount_usd = no_price_with_slippage * order_size;
        if yes_amount_usd <= dec!(1) || no_amount_usd <= dec!(1) {
            warn!(
                "⏭️ 跳过下单 | YES 金额:{:.2} USD NO 金额:{:.2} USD | 双边金额必须 > $1",
                yes_amount_usd, no_amount_usd
            );
            return Err(anyhow::anyhow!(
                "order amount below exchange minimum: YES {:.2} USD, NO {:.2} USD, both sides must be > $1",
                yes_amount_usd, no_amount_usd
            ));
        }

        // Dry run: simulate full fill, no actual orders
        if self.dry_run {
            info!(
                "[模拟模式] 模拟套利 | 市场:{} | 不流动侧:{} FOK {:.4} | 流动侧:{} GTD {:.4} | 数量:{}",
                opp.market_id,
                illiquid_label, illiquid_price,
                liquid_label, liquid_price,
                order_size
            );
            let yes_order_id = format!("dry-run-yes-{}", &pair_id[..8]);
            let no_order_id = format!("dry-run-no-{}", &pair_id[..8]);
            return Ok(OrderPairResult {
                pair_id,
                yes_order_id,
                no_order_id,
                yes_filled: order_size,
                no_filled: order_size,
                yes_size: order_size,
                no_size: order_size,
                success: true,
            });
        }

        let client = self.client.as_ref().unwrap();
        let signer = LocalSigner::from_str(&self.private_key)?
            .with_chain_id(Some(POLYGON));

        // ── Step 1: Build, sign, and send illiquid side with FOK ──
        let step1_start = Instant::now();

        let illiquid_order = client
            .limit_order()
            .token_id(illiquid_token)
            .side(Side::Buy)
            .price(illiquid_price)
            .size(order_size)
            .order_type(OrderType::FOK)
            .build()
            .await?;

        let signed_illiquid = client.sign(&signer, illiquid_order).await?;

        let illiquid_results = match client.post_orders(vec![signed_illiquid]).await {
            Ok(results) => results,
            Err(e) => {
                let elapsed = step1_start.elapsed().as_millis();
                error!(
                    "❌ 不流动侧 FOK 下单失败 | {} | 单对:{} | 价格:{} | 数量:{} | 用时:{}ms | 错误:{}",
                    illiquid_label, &pair_id[..8], illiquid_price, order_size, elapsed, e
                );
                return Err(anyhow::anyhow!("illiquid FOK order API call failed: {}", e));
            }
        };

        if illiquid_results.is_empty() {
            return Err(anyhow::anyhow!("illiquid FOK order returned empty result"));
        }

        let illiquid_result = &illiquid_results[0];
        let mut illiquid_filled = illiquid_result.taking_amount;
        let mut illiquid_order_id_str = illiquid_result.order_id.clone();
        let step1_elapsed = step1_start.elapsed().as_millis();

        // If FOK didn't fill → skip trade entirely (loss $0)
        if illiquid_filled == dec!(0) {
            let error_msg = illiquid_result.error_msg.as_deref().unwrap_or("no fill");
            info!(
                "⏭️ 不流动侧未成交 | {} FOK | 单对:{} | 原因:{} | 用时:{}ms | 尝试降级为 FAK",
                illiquid_label, &pair_id[..8], error_msg, step1_elapsed
            );
            // ── Fallback: try FAK for illiquid side (allow partial fill) ──
            let fallback_start = Instant::now();
            let illiquid_fak_order = client
                .limit_order()
                .token_id(illiquid_token)
                .side(Side::Buy)
                .price(illiquid_price)
                .size(order_size)
                .order_type(OrderType::FAK)
                .build()
                .await?;
            let signed_illiquid_fak = client.sign(&signer, illiquid_fak_order).await?;
            let illiquid_fak_results = match client.post_orders(vec![signed_illiquid_fak]).await {
                Ok(results) => results,
                Err(e) => {
                    let fb_elapsed = fallback_start.elapsed().as_millis();
                    info!(
                        "⏭️ 不流动侧 FAK 尝试失败 | {} | 单对:{} | 用时:{}ms | 错误:{} | 交易跳过",
                        illiquid_label, &pair_id[..8], fb_elapsed, e
                    );
                    return Ok(OrderPairResult {
                        pair_id,
                        yes_order_id: String::new(),
                        no_order_id: String::new(),
                        yes_filled: dec!(0),
                        no_filled: dec!(0),
                        yes_size: order_size,
                        no_size: order_size,
                        success: false,
                    });
                }
            };
            if illiquid_fak_results.is_empty() {
                let fb_elapsed = fallback_start.elapsed().as_millis();
                info!(
                    "⏭️ 不流动侧 FAK 返回空结果 | {} | 单对:{} | 用时:{}ms | 交易跳过",
                    illiquid_label, &pair_id[..8], fb_elapsed
                );
                return Ok(OrderPairResult {
                    pair_id,
                    yes_order_id: String::new(),
                    no_order_id: String::new(),
                    yes_filled: dec!(0),
                    no_filled: dec!(0),
                    yes_size: order_size,
                    no_size: order_size,
                    success: false,
                });
            }
            let fallback_result = &illiquid_fak_results[0];
            let fallback_filled = fallback_result.taking_amount;
            let fb_elapsed = fallback_start.elapsed().as_millis();
            if fallback_filled == dec!(0) {
                info!(
                    "⏭️ 不流动侧 FAK 未成交 | {} | 单对:{} | 用时:{}ms | 交易跳过",
                    illiquid_label, &pair_id[..8], fb_elapsed
                );
                return Ok(OrderPairResult {
                    pair_id,
                    yes_order_id: String::new(),
                    no_order_id: String::new(),
                    yes_filled: dec!(0),
                    no_filled: dec!(0),
                    yes_size: order_size,
                    no_size: order_size,
                    success: false,
                });
            }
            info!(
                "✅ 不流动侧部分成交 | {} FAK | 单对:{} | 成交:{} 股 | 用时:{}ms",
                illiquid_label, &pair_id[..8], fallback_filled, fb_elapsed
            );
            // Use fallback result as the illiquid fill and proceed to liquid step
            illiquid_filled = fallback_filled;
            illiquid_order_id_str = fallback_result.order_id.clone();
        }

        info!(
            "✅ 不流动侧成交 | {} FOK | 单对:{} | 成交:{} 股 | 用时:{}ms",
            illiquid_label, &pair_id[..8], illiquid_filled, step1_elapsed
        );

        // ── Step 2: Build, sign, and send liquid side with GTD ──
        let step2_start = Instant::now();
        let expiration = Utc::now() + chrono::Duration::seconds(self.gtd_expiration_secs as i64);

        let liquid_order = client
            .limit_order()
            .token_id(liquid_token)
            .side(Side::Buy)
            .price(liquid_price)
            .size(order_size)
            .order_type(OrderType::GTD)
            .expiration(expiration)
            .build()
            .await?;

        let signed_liquid = client.sign(&signer, liquid_order).await?;

        let liquid_results = match client.post_orders(vec![signed_liquid]).await {
            Ok(results) => results,
            Err(e) => {
                let step2_elapsed = step2_start.elapsed().as_millis();
                let total_elapsed = total_start.elapsed().as_millis();
                error!(
                    "❌ 流动侧 GTD 下单失败 | {} | 单对:{} | 价格:{} | 数量:{} | 步骤2用时:{}ms | 总用时:{}ms | 错误:{}",
                    liquid_label, &pair_id[..8], liquid_price, order_size, step2_elapsed, total_elapsed, e
                );
                // Illiquid side already filled — this is a one-sided fill, return it so risk manager can handle
                let (yes_filled, no_filled) = if yes_is_illiquid {
                    (illiquid_filled, dec!(0))
                } else {
                    (dec!(0), illiquid_filled)
                };
                warn!(
                    "⚠️ 单侧成交 | {} | {} 成交 {} 股，{} 下单失败（已转交风险管理）",
                    &pair_id[..8], illiquid_label, illiquid_filled, liquid_label
                );
                return Ok(OrderPairResult {
                    pair_id,
                    yes_order_id: if yes_is_illiquid { illiquid_result.order_id.clone() } else { String::new() },
                    no_order_id: if yes_is_illiquid { String::new() } else { illiquid_result.order_id.clone() },
                    yes_filled,
                    no_filled,
                    yes_size: order_size,
                    no_size: order_size,
                    success: true,
                });
            }
        };

        if liquid_results.is_empty() {
            let (yes_filled, no_filled) = if yes_is_illiquid {
                (illiquid_filled, dec!(0))
            } else {
                (dec!(0), illiquid_filled)
            };
            warn!(
                "⚠️ 单侧成交 | {} | {} 成交 {} 股，{} 返回空结果（已转交风险管理）",
                &pair_id[..8], illiquid_label, illiquid_filled, liquid_label
            );
            return Ok(OrderPairResult {
                pair_id,
                yes_order_id: if yes_is_illiquid { illiquid_result.order_id.clone() } else { String::new() },
                no_order_id: if yes_is_illiquid { String::new() } else { illiquid_result.order_id.clone() },
                yes_filled,
                no_filled,
                yes_size: order_size,
                no_size: order_size,
                success: true,
            });
        }

        let liquid_result = &liquid_results[0];
        let liquid_filled = liquid_result.taking_amount;
        let step2_elapsed = step2_start.elapsed().as_millis();
        let total_elapsed = total_start.elapsed().as_millis();

        info!(
            "⏱️ 用时 | {} | 步骤1(FOK) {}ms 步骤2(GTD) {}ms 总计 {}ms",
            &pair_id[..8], step1_elapsed, step2_elapsed, total_elapsed
        );

        // Map back to YES/NO
        let (yes_filled, no_filled) = if yes_is_illiquid {
            (illiquid_filled, liquid_filled)
        } else {
            (liquid_filled, illiquid_filled)
        };
        let (yes_order_id, no_order_id) = if yes_is_illiquid {
            (illiquid_order_id_str.clone(), liquid_result.order_id.clone())
        } else {
            (liquid_result.order_id.clone(), illiquid_order_id_str.clone())
        };

        // Print fill status
        if yes_filled > dec!(0) && no_filled > dec!(0) {
            info!(
                "✅ 套利成功 | 单对:{} | YES 成交:{} 股 | NO 成交:{} 股 | 总成交:{} 股",
                &pair_id[..8],
                yes_filled,
                no_filled,
                yes_filled.min(no_filled)
            );
        } else {
            // Illiquid filled but liquid didn't — one-sided fill (rare with GTD, handled by risk manager)
            let unfilled_side = if yes_filled == dec!(0) { "YES" } else { "NO" };
            warn!(
                "⚠️ 单侧成交 | {} | {} 未成交（GTD 挂单中，已转交风险管理）",
                &pair_id[..8], unfilled_side
            );
        }

        Ok(OrderPairResult {
            pair_id,
            yes_order_id,
            no_order_id,
            yes_filled,
            no_filled,
            yes_size: order_size,
            no_size: order_size,
            success: true,
        })
    }
}
