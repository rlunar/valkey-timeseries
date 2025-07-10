use crate::common::db::{get_current_db, set_current_db};
use crate::series::index::*;
use crate::series::{get_timeseries_mut, with_timeseries_mut, TimeSeries};
use std::os::raw::c_void;
use std::sync::Mutex;
use valkey_module::{logging, raw, Context, NotifyEvent, ValkeyError, ValkeyResult};

static RENAME_FROM_KEY: Mutex<Vec<u8>> = Mutex::new(vec![]);
static MOVE_FROM_DB: Mutex<i32> = Mutex::new(-1);

fn handle_key_restore(ctx: &Context, key: &[u8]) {
    let _key = ctx.create_string(key);
    with_timeseries_mut(ctx, &_key, None, |series| {
        series._db = get_current_db(ctx);
        reindex_series(ctx, series, key)
    })
    .expect("Unexpected panic handling series restore");
}

fn reindex_series(ctx: &Context, series: &TimeSeries, key: &[u8]) -> ValkeyResult<()> {
    with_timeseries_index(ctx, |index| {
        index.reindex_timeseries(series, key);
        Ok(())
    })
}

fn handle_key_rename(ctx: &Context, old_key: &[u8], new_key: &[u8]) {
    with_timeseries_index(ctx, |index| {
        index.rename_series(old_key, new_key);
    })
}

fn remove_key_from_index(ctx: &Context, key: &[u8]) {
    with_timeseries_index(ctx, |ts_index| {
        // At this point, the key has already been deleted by Valkey, so we no longer have access to the
        // labels and values of the series. We mark the series as deleted in the index and schedule "gc"
        // run which scans the index and removes the id
        ts_index.mark_key_as_stale(key)
    });
}

fn handle_loaded(ctx: &Context, key: &[u8]) {
    let _key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &_key, false, None) else {
        logging::log_warning("Failed to load series");
        return;
    };

    with_timeseries_index(ctx, |index| {
        series._db = get_current_db(ctx);
        if !index.has_id(series.id) {
            index.index_timeseries(&series, key);
            // On module load, our series id generator would have been reset to zero. We have to ensure
            // that after a load the counter has the value of the highest series id
            TIMESERIES_ID.fetch_max(series.id, std::sync::atomic::Ordering::Relaxed);
        } else {
            logging::log_warning("Trying to load a series that is already in the index");
        }
    });
}

fn handle_key_move(ctx: &Context, key: &[u8], old_db: i32) {
    let new_db = get_current_db(ctx);
    // fetch the series from the new
    let valkey_key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        return;
    };
    series._db = new_db;

    // remove the series from the old db index
    set_current_db(ctx, old_db);
    with_timeseries_index(ctx, |index| {
        index.remove_timeseries(&series);
    });

    // add the series to the new db index
    set_current_db(ctx, new_db);
    with_timeseries_index(ctx, |index| {
        index.index_timeseries(&series, key);
    });
}

// handle events that require removing the series from the index
pub(super) fn remove_key_events_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    ctx.log_notice(&format!(
        "Received event: {} for key: {}",
        event,
        String::from_utf8_lossy(key)
    ));
    // If the event is one of the ones that require removing the series from the index, we
    // remove it from the index
    if hashify::tiny_set!(
        event.as_bytes(),
        "del",
        "evict",
        "evicted",
        "expire",
        "expired",
        "set",
        "trimmed"
    ) {
        remove_key_from_index(ctx, key);
    }
}

pub(super) fn generic_key_events_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    // todo: AddPostNotificationJob(ctx, event, key);
    ctx.log_notice(&format!(
        "Received event: {} for key: {}",
        event,
        String::from_utf8_lossy(key)
    ));

    hashify::fnc_map_ignore_case!(event.as_bytes(),
        "loaded" => {
            ctx.log_notice("Loaded event received");
            handle_loaded(ctx, key);
        },
        "move_from" => {
            *MOVE_FROM_DB.lock().unwrap() = get_current_db(ctx);
        },
        "move_to" => {
            let mut lock = MOVE_FROM_DB.lock().unwrap();
            let old_db = *lock;
            *lock = -1;
            if old_db != -1 {
                handle_key_move(ctx, key, old_db);
            }
        },
        "rename_from" => {
            *RENAME_FROM_KEY.lock().unwrap() = key.to_vec();
        },
        "rename_to" => {
            let mut old_key = RENAME_FROM_KEY.lock().unwrap();
            if !old_key.is_empty() {
                handle_key_rename(ctx, &old_key, key);
                old_key.clear();
            }
        },
        "restore" => {
            handle_key_restore(ctx, key);
        },
        _ => {}
    );
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let ctx = Context::new(ctx);
        let fi: &raw::RedisModuleFlushInfo = unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            clear_all_timeseries_indexes();
        } else {
            clear_timeseries_index(&ctx);
        }
    };
}

unsafe extern "C" fn on_swap_db_event(
    _ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    _sub_event: u64,
    data: *mut c_void,
) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };

        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(from_db, to_db);
    }
}

fn register_server_event_handler(
    ctx: &Context,
    server_event: u64,
    inner_callback: raw::RedisModuleEventCallback,
) -> Result<(), ValkeyError> {
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: server_event,
                dataver: 1,
            },
            inner_callback,
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        return Err(ValkeyError::Str("Failed subscribing to server event"));
    }

    Ok(())
}

pub fn register_server_events(ctx: &Context) -> ValkeyResult<()> {
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    Ok(())
}
