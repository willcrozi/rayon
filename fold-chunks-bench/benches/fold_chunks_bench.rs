use rayon::iter::{ParallelIterator, IndexedParallelIterator, IntoParallelIterator};
use itertools::Itertools;

use criterion::*;

// noop benchmark fns

fn seq_chunks_fold_noop(count: usize, chunk_len: usize) {
    (0..count)
        .chunks(chunk_len).into_iter()
        .fold((), |_, chunk | for x in chunk { black_box(x); });
}

// included for comparison (but doesn't perform chunking)
fn par_fold_noop(count: usize, _chunk_len: usize) {
    (0..count).into_par_iter()
        .fold(|| (), |_, x | { black_box(x); })
        .for_each(drop);
}

fn par_chunks_fold_noop(count: usize, chunk_len: usize) {
    (0..count).into_par_iter()
        .chunks(chunk_len)
        .fold(|| (), |_, chunk | for x in chunk { black_box(x); })
        .for_each(drop);
}

fn par_chunks_foreach_noop(count: usize, chunk_len: usize) {
    (0..count).into_par_iter()
        .chunks(chunk_len)
        .for_each(|chunk| for x in chunk { black_box(x); })
}

fn par_foldchunks_noop(count: usize, chunk_len: usize) {
    (0..count).into_par_iter()
        .fold_chunks(chunk_len, || (), |_, x| { black_box(x); })
        .for_each(drop);
}

fn par_foldchunkswith_noop(count: usize, chunk_len: usize) {
    (0..count).into_par_iter()
        .fold_chunks_with(chunk_len, (), |_, x| { black_box(x); })
        .for_each(drop);
}

// sum benchmark fns

const fn sum_check(count: usize) -> usize {
    // using non-inclusive ranges, hence: sum(0..n) == ((n - 1) * n) / 2
    ((count - 1) * count) / 2
}

fn seq_chunks_fold_sum(count: usize, chunk_len: usize) -> usize {
    let sum = (0..count)
        .chunks(chunk_len).into_iter()
        .fold(0, |acc, chunk| acc + chunk.sum::<usize>());

    debug_assert_eq!(sum, sum_check(count));
    sum
}

// included for comparison (but doesn't perform chunking)
fn par_fold_sum(count: usize, _chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .fold(|| 0, |acc, x| acc + x)
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}

// included for comparison (but doesn't perform chunking)
fn par_sum(count: usize, _chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}

fn par_chunks_fold_sum(count: usize, chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .chunks(chunk_len)
        .fold(|| 0, |acc, chunk| acc + chunk.into_iter().sum::<usize>())
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}

fn par_chunks_map_sum(count: usize, chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .chunks(chunk_len)
        .map(|chunk| chunk.into_iter().sum::<usize>())
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}

fn par_foldchunks_sum(count: usize, chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .fold_chunks(chunk_len, || 0, |acc, n| acc + n)
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}

fn par_foldchunkswith_sum(count: usize, chunk_len: usize) -> usize {
    let sum: usize = (0..count).into_par_iter()
        .fold_chunks_with(chunk_len, 0, |acc, n| acc + n)
        .sum();

    debug_assert_eq!(sum, sum_check(count));
    sum
}


const K: usize = 1024;
const M: usize = 1024 * K;

const ELEM_COUNT: usize = 8 * M;

static CHUNK_LENS: &[usize] = &[
        1,     2,     4,     8,
       16,    32,    64,   128,
      256,   512,   1*K,   2*K,
      4*K,   8*K,  16*K,  32*K,
     64*K, 128*K, 256*K, 512*K,
      1*M,   2*M,   4*M,   8*M,
];

fn noop_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("noop_bench");

    // group.throughput(Throughput::Elements(ELEM_COUNT as u64));

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    group.plot_config(plot_config);

    for chunk_len in CHUNK_LENS.iter() {
        group.bench_with_input(BenchmarkId::new("seq_chunks_fold_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| seq_chunks_fold_noop(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_fold_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_fold_noop(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_chunks_fold_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_chunks_fold_noop(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_chunks_foreach_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_chunks_foreach_noop(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_foldchunks_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_foldchunks_noop(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_foldchunkswith_noop", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_foldchunkswith_noop(ELEM_COUNT, black_box(chunk_len))));
    }

    group.finish();
}

fn sum_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_bench");

    // group.throughput(Throughput::Elements(ELEM_COUNT as u64));

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    group.plot_config(plot_config);

    for chunk_len in CHUNK_LENS.iter() {

        group.bench_with_input(BenchmarkId::new("seq_chunks_fold_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| seq_chunks_fold_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_fold_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_fold_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_chunks_fold_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_chunks_fold_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_chunks_map_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_chunks_map_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_foldchunks_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_foldchunks_sum(ELEM_COUNT, black_box(chunk_len))));

        group.bench_with_input(BenchmarkId::new("par_foldchunkswith_sum", chunk_len), chunk_len,
            |b, &chunk_len| b.iter(|| par_foldchunkswith_sum(ELEM_COUNT, black_box(chunk_len))));
    }

    group.finish();
}

criterion_group!(benches, noop_bench, sum_bench);
criterion_main!(benches);
