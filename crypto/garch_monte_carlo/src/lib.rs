// garch_monte_carlo/src/lib.rs
// Cargo.toml dependencies:
// [dependencies]
// pyo3 = { version = "0.20", features = ["extension-module"] }
// rayon = "1.8"
// rand = "0.8"
// rand_xoshiro = "0.6"

use pyo3::prelude::*;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use rayon::prelude::*;

#[pyfunction]
fn calculate_probability_only(
    omega: f64,
    alpha: f64,
    beta: f64,
    last_resid: f64,
    last_sigma_sq: f64,
    residuals: Vec<f64>,
    current_price: f64,
    target_price: f64,
    horizon_minutes: usize,
    num_simulations: usize,
) -> PyResult<f64> {
    let initial_sigma_sq = omega + alpha * last_resid.powi(2) + beta * last_sigma_sq;
    let residuals_len = residuals.len();

    // Count successes without storing all prices (saves memory)
    let count_above: usize = (0..num_simulations)
        .into_par_iter()
        .map_init(
            || Xoshiro256PlusPlus::from_entropy(),
            |rng, _| {
                let mut price = current_price;
                let mut current_sigma_sq = initial_sigma_sq;

                for _ in 0..horizon_minutes {
                    let idx = rng.gen_range(0..residuals_len);
                    let shock = residuals[idx];
                    let sigma = current_sigma_sq.sqrt();
                    let simulated_return = sigma * shock;
                    price *= (simulated_return).exp();
                    current_sigma_sq = omega + alpha * shock * shock + beta * current_sigma_sq;
                }

                (price > target_price) as usize
            },
        )
        .sum();

    Ok(count_above as f64 / num_simulations as f64)
}

#[pyfunction]
fn calculate_probability_plain(
    returns: Vec<f64>,
    current_price: f64,
    target_price: f64,
    horizon_minutes: usize,
    num_simulations: usize,
) -> PyResult<f64> {
    let returns_len = returns.len();

    let count_above: usize = (0..num_simulations)
        .into_par_iter()
        .map_init(
            || Xoshiro256PlusPlus::from_entropy(),
            |rng, _| {
                let mut price = current_price;

                for _ in 0..horizon_minutes {
                    let idx = rng.gen_range(0..returns_len);
                    let simulated_return = returns[idx];
                    price *= 1.0 + simulated_return;
                }

                (price > target_price) as usize
            },
        )
        .sum();

    Ok(count_above as f64 / num_simulations as f64)
}

#[pymodule]
fn garch_monte_carlo(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calculate_probability_plain, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_probability_only, m)?)?;
    Ok(())
}


