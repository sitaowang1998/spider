//! Measures construction, compression, and decoding of submitted task graph inputs.

use std::hint::black_box;
use std::time::Duration;
use std::time::Instant;

use spider_core::compression::decode_zstd_bytes;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskGraphInputError;

/// A task graph input workload.
struct Case {
    normal_inputs: usize,
    payload_size: usize,
    shared: bool,
    termination_inputs: bool,
}

impl Case {
    /// Creates the configured input lists from one payload.
    ///
    /// # Returns
    ///
    /// The constructed task graph inputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskGraphInputBuilder::create_shared_input_payload`]'s return values on
    ///   failure.
    /// * Forwards [`TaskGraphInputBuilder::append_shared_task_input`]'s return values on failure.
    /// * Forwards [`TaskGraphInputBuilder::append_shared_commit_task_input`]'s return values on
    ///   failure.
    /// * Forwards [`TaskGraphInputBuilder::append_shared_cleanup_task_input`]'s return values on
    ///   failure.
    /// * Forwards [`TaskGraphInputBuilder::append_task_input`]'s return values on failure.
    /// * Forwards [`TaskGraphInputBuilder::append_commit_task_input`]'s return values on failure.
    /// * Forwards [`TaskGraphInputBuilder::append_cleanup_task_input`]'s return values on failure.
    fn build(&self, payload: &[u8]) -> Result<TaskGraphInput, TaskGraphInputError> {
        let mut builder = TaskGraphInputBuilder::new();
        if self.shared {
            let id = builder.create_shared_input_payload(payload)?;
            for _ in 0..self.normal_inputs {
                builder.append_shared_task_input(id)?;
            }
            if self.termination_inputs {
                builder.append_shared_commit_task_input(id)?;
                builder.append_shared_cleanup_task_input(id)?;
            }
        } else {
            for _ in 0..self.normal_inputs {
                builder.append_task_input(payload)?;
            }
            if self.termination_inputs {
                builder.append_commit_task_input(payload)?;
                builder.append_cleanup_task_input(payload)?;
            }
        }
        Ok(builder.build())
    }

    /// Measures and prints construction, encoding, and decoding costs for this workload.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::build`]'s return values on failure.
    /// * Forwards [`TaskGraphInput::to_zstd_compressed_bytes`]'s return values on failure.
    /// * Forwards [`TaskGraphInput::from_zstd_compressed_bytes`]'s return values on failure.
    /// * Forwards [`decode_zstd_bytes`]'s return values on failure.
    /// * Forwards [`measure`]'s return values on failure.
    fn run(&self) -> Result<(), TaskGraphInputError> {
        let payload = make_payload(self.payload_size);
        let inputs = self.build(&payload)?;
        let compressed = inputs.to_zstd_compressed_bytes()?;
        let wire_size = decode_zstd_bytes(&compressed)?.len();
        assert_eq!(
            TaskGraphInput::from_zstd_compressed_bytes(&compressed)?,
            inputs,
            "benchmark input must round-trip"
        );
        let build_ns = measure(|| {
            black_box(self.build(black_box(&payload))?);
            Ok(())
        })?;
        let encode_ns = measure(|| {
            black_box(black_box(&inputs).to_zstd_compressed_bytes()?);
            Ok(())
        })?;
        let decode_ns = measure(|| {
            black_box(TaskGraphInput::from_zstd_compressed_bytes(black_box(
                &compressed,
            ))?);
            Ok(())
        })?;
        println!(
            "{},{},{},{},{},{wire_size},{build_ns:.1},{encode_ns:.1},{decode_ns:.1}",
            self.normal_inputs,
            self.payload_size,
            self.shared,
            self.termination_inputs,
            compressed.len(),
        );
        Ok(())
    }
}

/// Runs input workloads, optionally restricted to the existing normal-input API.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`Case::run`]'s return values on failure.
fn main() -> Result<(), TaskGraphInputError> {
    let normal_only = std::env::args().any(|arg| arg == "--normal-only");
    println!(
        "normal_inputs,payload_size,shared,termination_inputs,compressed_size,wire_size,build_ns,\
         encode_ns,decode_ns"
    );
    Case {
        normal_inputs: 0,
        payload_size: 0,
        shared: false,
        termination_inputs: false,
    }
    .run()?;
    for payload_size in [256, 4_096, 65_536] {
        for shared in [false, true] {
            for termination_inputs in [false, true] {
                if normal_only && termination_inputs {
                    continue;
                }
                Case {
                    normal_inputs: 64,
                    payload_size,
                    shared,
                    termination_inputs,
                }
                .run()?;
            }
        }
    }
    Ok(())
}

/// Measures the median duration of an operation across seven samples of at least 100 ms each.
///
/// Durations include destruction of the operation's result and the loop's elapsed-time check.
///
/// # Returns
///
/// The median duration per operation, in nanoseconds, on success.
///
/// # Errors
///
/// Returns an error if the supplied operation fails.
fn measure(
    mut operation: impl FnMut() -> Result<(), TaskGraphInputError>,
) -> Result<f64, TaskGraphInputError> {
    const SAMPLE_DURATION: Duration = Duration::from_millis(100);
    for _ in 0..20 {
        operation()?;
    }
    let mut samples = Vec::with_capacity(7);
    for _ in 0..7 {
        let start = Instant::now();
        let mut count = 0u32;
        loop {
            operation()?;
            count += 1;
            let elapsed = start.elapsed();
            if elapsed >= SAMPLE_DURATION {
                samples.push(elapsed.as_secs_f64() * 1e9 / f64::from(count));
                break;
            }
        }
    }
    samples.sort_by(f64::total_cmp);
    Ok(samples[samples.len() / 2])
}

/// Creates deterministic bytes with little repetition for compression measurements.
///
/// # Returns
///
/// The generated payload with the requested length.
fn make_payload(size: usize) -> Vec<u8> {
    let mut state = 0x1234_5678u32;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect()
}
