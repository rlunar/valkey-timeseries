use crate::common::context::{get_current_db, set_current_db};
use crate::common::hash::{BuildNoHashHasher, IntMap};
use crate::common::logging::{log_debug, log_warning};
use crate::common::threads::spawn;
use crate::series::index::{
    IndexKey, TIMESERIES_INDEX, get_db_index, get_timeseries_index, with_timeseries_postings,
};
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, get_timeseries_mut};
use blart::AsBytes;
use orx_parallel::{IntoParIter, ParIter, ParallelizableCollection, ParallelizableCollectionMut};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, DetachedFromClient, Status, ThreadSafeContext};
use valkey_module_macros::{cron_event_handler, shutdown_event_handler};

const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const SERIES_TRIM_BATCH_SIZE: usize = 50;
const STALE_ID_CLEANUP_INTERVAL: Duration = Duration::from_secs(15);
const STALE_ID_BATCH_SIZE: usize = 25;
const INDEX_OPTIMIZE_BATCH_SIZE: usize = 50;
const INDEX_OPTIMIZE_INTERVAL: Duration = Duration::from_secs(60);
const DB_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Default)]
struct IndexMeta {
    stale_id_cursor: Option<IndexKey>,
    optimize_cursor: Option<IndexKey>,
    last_updated: u64,
}

type IndexCursorMap = std::collections::HashMap<i32, IndexMeta, BuildNoHashHasher<i32>>;
type SeriesCursorMap = IntMap<i32, SeriesRef>;

type DispatchMap =
    papaya::HashMap<u64, Vec<fn(&ThreadSafeContext<DetachedFromClient>)>, BuildNoHashHasher<u64>>;

static CRON_TICKS: AtomicU64 = AtomicU64::new(0);
static CRON_INTERVAL_MS: AtomicU64 = AtomicU64::new(100);

static SERIES_TRIM_CURSORS: LazyLock<Mutex<SeriesCursorMap>> =
    LazyLock::new(|| Mutex::new(SeriesCursorMap::default()));
static INDEX_CURSORS: LazyLock<Mutex<IndexCursorMap>> =
    LazyLock::new(|| Mutex::new(IndexCursorMap::default()));

static CURRENT_DB: AtomicI32 = AtomicI32::new(0);

static DISPATCH_MAP: LazyLock<DispatchMap> = LazyLock::new(|| DispatchMap::default());
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

const INDEX_LOCK_POISON_MSG: &str = "Failed to lock INDEX_CURSORS";
const MAX_TRIM_TURNS: usize = 5;

pub(crate) fn init_background_tasks(ctx: &Context) {
    let interval_ms = get_ticks_interval(ctx);
    CRON_INTERVAL_MS.store(interval_ms, Ordering::Relaxed);

    let cron_interval = Duration::from_millis(interval_ms);

    register_task(ctx, RETENTION_CLEANUP_INTERVAL, cron_interval, process_trim);
    register_task(
        ctx,
        STALE_ID_CLEANUP_INTERVAL,
        cron_interval,
        process_remove_stale_series,
    );
    register_task(
        ctx,
        INDEX_OPTIMIZE_INTERVAL,
        cron_interval,
        optimize_indices,
    );
    register_task(ctx, DB_CLEANUP_INTERVAL, cron_interval, trim_unused_dbs);
}

fn register_task(
    ctx: &Context,
    task_interval: Duration,
    cron_interval: Duration,
    f: fn(&ThreadSafeContext<DetachedFromClient>),
) {
    if cron_interval.is_zero() {
        panic!("register_task: cron_interval is zero");
    }

    let floored = floor_duration(task_interval, cron_interval);
    if floored.is_zero() {
        ctx.log_warning(&format!(
            "register_task: interval is zero (task_interval={task_interval:?}, cron_interval={cron_interval:?})",
        ));
        return;
    }

    let interval_ticks = ticks_for_interval(floored, cron_interval);

    let map = DISPATCH_MAP.pin();
    map.update_or_insert_with(
        interval_ticks,
        |handlers| {
            if handlers.iter().any(|&h| h as usize == f as usize) {
                handlers.clone()
            } else {
                let mut v = handlers.clone();
                v.push(f);
                v
            }
        },
        || vec![f],
    );
}

#[inline]
fn ticks_for_interval(task_interval: Duration, cron_interval: Duration) -> u64 {
    let task_ms = task_interval.as_millis() as u64;
    let cron_ms = cron_interval.as_millis() as u64;
    std::cmp::max(1, task_ms / cron_ms)
}

fn floor_duration(d: Duration, interval: Duration) -> Duration {
    if interval.is_zero() {
        return Duration::ZERO;
    }
    let nanos = d.as_nanos();
    let int_nanos = interval.as_nanos();
    let floored = (nanos / int_nanos) * int_nanos;

    if floored == 0 {
        return Duration::ZERO;
    }

    // Saturate instead of truncating if it does not fit u64 nanos.
    if floored > u64::MAX as u128 {
        Duration::from_nanos(u64::MAX)
    } else {
        Duration::from_nanos(floored as u64)
    }
}

fn get_used_dbs() -> Vec<i32> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: Vec<i32> = index.keys().copied().collect();
    keys.sort_unstable();
    keys
}

#[inline]
fn with_series_trim_cursors<T>(f: impl FnOnce(&mut SeriesCursorMap) -> T) -> T {
    let mut map = SERIES_TRIM_CURSORS.lock().unwrap();
    f(&mut map)
}

#[inline]
fn with_index_cursors<T>(f: impl FnOnce(&mut IndexCursorMap) -> T) -> T {
    let mut map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
    f(&mut map)
}

#[inline]
fn with_index_meta<T>(db: i32, f: impl FnOnce(&mut IndexMeta) -> T) -> T {
    with_index_cursors(|map| {
        let meta = map.entry(db).or_default();
        f(meta)
    })
}

#[inline]
fn get_stale_id_cursor(db: i32) -> Option<IndexKey> {
    with_index_cursors(|map| map.get(&db).and_then(|meta| meta.stale_id_cursor.clone()))
}

#[inline]
fn set_stale_id_cursor(db: i32, cursor: Option<IndexKey>) {
    with_index_meta(db, |meta| {
        meta.stale_id_cursor = cursor;
    })
}

#[inline]
fn get_trim_cursor(db: i32) -> SeriesRef {
    with_series_trim_cursors(|map| *map.entry(db).or_default())
}

#[inline]
fn set_trim_cursor(db: i32, cursor: SeriesRef) {
    with_series_trim_cursors(|map| {
        *map.entry(db).or_default() = cursor;
    })
}

#[inline]
fn get_optimize_cursor(db: i32) -> Option<IndexKey> {
    with_index_cursors(|map| map.get(&db).and_then(|meta| meta.optimize_cursor.clone()))
}

#[inline]
fn set_optimize_cursor(db: i32, cursor: Option<IndexKey>) {
    with_index_meta(db, |meta| {
        meta.optimize_cursor = cursor;
    })
}

fn next_db() -> i32 {
    let used_dbs = get_used_dbs();
    let current = CURRENT_DB.load(Ordering::Relaxed);

    let next = used_dbs
        .iter()
        .find(|&&d| d > current)
        .copied()
        .or_else(|| used_dbs.first().copied())
        .unwrap_or(0);

    CURRENT_DB.store(next, Ordering::Relaxed);
    next
}

fn process_trim(ctx: &ThreadSafeContext<DetachedFromClient>) {
    let mut processed = 0;
    let start_db = next_db();
    let mut db = start_db;

    for _ in 0..MAX_TRIM_TURNS {
        if is_shutting_down() {
            break;
        }

        processed += trim_series(ctx, db);

        if processed >= SERIES_TRIM_BATCH_SIZE {
            break;
        }

        db = next_db();
        if db == start_db {
            break;
        }
    }
}

fn trim_series(ctx: &ThreadSafeContext<DetachedFromClient>, db: i32) -> usize {
    let cursor = get_trim_cursor(db);

    let ctx_ = ctx.lock();
    if set_current_db(&ctx_, db) == Status::Err {
        log_warning(format!("Failed to select db {db}"));
        return 0;
    }

    let mut batch = fetch_series_batch(&ctx_, cursor + 1, |series| {
        !series.retention.is_zero() && !series.is_empty()
    });

    if batch.is_empty() {
        set_trim_cursor(db, 0);
        return 0;
    }

    let last_processed = batch.last().map(|s| s.id).unwrap_or(0);
    let processed = batch.len();

    let total_deletes = batch
        .par_mut()
        .map(|series| match series.trim() {
            Ok(deletes) => deletes,
            Err(_) => {
                log_warning(format!(
                    "Failed to trim series {}",
                    series.prometheus_metric_name()
                ));
                0
            }
        })
        .sum();

    drop(ctx_);

    set_trim_cursor(db, last_processed);

    if processed > 0 {
        log_debug(format!(
            "Processed: {processed} Deleted Samples: {total_deletes} samples"
        ));
    }

    processed
}

fn fetch_series_batch(
    ctx: &'_ Context,
    start_id: SeriesRef,
    pred: fn(&TimeSeries) -> bool,
) -> Vec<SeriesGuardMut<'_>> {
    let (result, stale_ids) = with_timeseries_postings(ctx, |postings| {
        let all_postings = &postings.all_postings;
        let mut cursor = all_postings.cursor();
        cursor.reset_at_or_after(start_id);

        let mut buf = [0_u64; SERIES_TRIM_BATCH_SIZE];
        let n = cursor.read_many(&mut buf);
        if n == 0 {
            return (vec![], vec![]);
        }

        let mut stale_ids = Vec::new();
        let mut result = Vec::with_capacity(n);

        for &id in &buf[..n] {
            let Some(k) = postings.get_key_by_id(id) else {
                continue;
            };

            let key = ctx.create_string(k.as_bytes());
            let Ok(Some(series)) = get_timeseries_mut(ctx, &key, false, None) else {
                stale_ids.push(id);
                continue;
            };

            if pred(&series) {
                result.push(series);
            }
        }

        (result, stale_ids)
    });

    if !stale_ids.is_empty() {
        let index = get_timeseries_index(ctx);
        for id in stale_ids {
            index.mark_id_as_stale(id)
        }
    }

    result
}

fn remove_stale_series_internal(db: i32) {
    if is_shutting_down() {
        return;
    }
    if let Some(index) = TIMESERIES_INDEX.pin().get(&db) {
        let mut state = 0;
        let cursor = get_stale_id_cursor(db);
        let was_none = cursor.is_none();

        index.with_postings_mut(&mut state, move |postings, _| {
            let new_cursor = postings.remove_stale_ids(cursor, STALE_ID_BATCH_SIZE);
            if new_cursor.is_some() {
                // if we have a new cursor, we need to update it
                set_stale_id_cursor(db, new_cursor);
            } else if !was_none {
                // if we were not given a cursor, we need to set it to None, but only if it was not
                // already None
                set_stale_id_cursor(db, None);
            }
        });
    }
}

fn process_remove_stale_series(_ctx: &ThreadSafeContext<DetachedFromClient>) {
    if is_shutting_down() {
        return;
    }

    let db_ids = get_used_dbs();
    if db_ids.is_empty() {
        return;
    }

    db_ids
        .par()
        .for_each(|&db| remove_stale_series_internal(db));
}

fn optimize_indices(_ctx: &ThreadSafeContext<DetachedFromClient>) {
    if is_shutting_down() {
        return;
    }

    let db_ids = get_used_dbs();
    if db_ids.is_empty() {
        return;
    }

    let cursors: Vec<(i32, Option<IndexKey>)> = db_ids
        .iter()
        .map(|&db| (db, get_optimize_cursor(db)))
        .collect();

    let results = cursors
        .into_par()
        .map(|(db, cursor)| {
            let index = get_db_index(db);
            let new_cursor = index.optimize_incremental(cursor, INDEX_OPTIMIZE_BATCH_SIZE);
            (db, new_cursor)
        })
        .collect::<Vec<_>>();

    for (db, new_cursor) in results {
        set_optimize_cursor(db, new_cursor);
    }
}

fn trim_unused_dbs(_ctx: &ThreadSafeContext<DetachedFromClient>) {
    let mut index = TIMESERIES_INDEX.pin();
    index.retain(|db, ts_index| {
        if ts_index.is_empty() {
            log_debug(format!("Removing unused db {db} from index"));
            false
        } else {
            true
        }
    });
}

#[cron_event_handler]
fn cron_event_handler(ctx: &Context, _hz: u64) {
    if is_shutting_down() {
        return;
    }

    let ticks = CRON_TICKS.fetch_add(1, Ordering::Relaxed);
    let save_db = get_current_db(ctx);

    dispatch_tasks(ticks);

    set_current_db(ctx, save_db);
}

fn dispatch_tasks(ticks: u64) {
    let map = DISPATCH_MAP.pin();

    // Collect tasks to run without holding the pinned map longer than needed.
    let mut tasks: Vec<fn(&ThreadSafeContext<DetachedFromClient>)> = Vec::new();
    for (&interval, handlers) in map.iter() {
        if ticks.is_multiple_of(interval) {
            tasks.extend(handlers.iter().copied());
        }
    }

    drop(map);

    if tasks.is_empty() {
        return;
    }

    log_debug(format!("cron_event_handler: tasks={}", tasks.len()));

    for task in tasks {
        let thread_ctx = ThreadSafeContext::new();
        spawn(move || task(&thread_ctx));
    }
}

fn get_hz(ctx: &Context) -> u64 {
    let server_info = ctx.server_info("");
    server_info
        .field("hz")
        .and_then(|v| v.parse_integer().ok())
        .unwrap_or(10) as u64
}

fn get_ticks_interval(ctx: &Context) -> u64 {
    get_ticks_interval_from_hz(get_hz(ctx))
}

#[inline]
fn get_ticks_interval_from_hz(hz: u64) -> u64 {
    let hz = if hz == 0 { 10 } else { hz };
    1000 / hz
}

#[inline]
fn is_shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::Relaxed)
}

#[shutdown_event_handler]
fn shutdown_event_handler(ctx: &Context, _event: u64) {
    ctx.log_notice("Sever shutdown callback event ...");
    SHUTTING_DOWN.store(true, Ordering::Relaxed);
}
