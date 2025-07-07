use valkey_module::REDISMODULE_AUX_BEFORE_RDB;
use valkey_module::{logging, RedisModuleTypeMethods};
use valkey_module::{
    native_types::ValkeyType, RedisModuleDefragCtx, RedisModuleString, ValkeyString,
};

use crate::common::db::{get_current_db, set_current_db};
use crate::series::defrag_series;
use crate::series::index::{next_timeseries_id, with_timeseries_index};
use crate::series::serialization::{rdb_load_series, rdb_save_series};
use crate::series::TimeSeries;
use std::os::raw::{c_int, c_void};
use valkey_module::raw;

/// TimeSeries Module data type RDB encoding version.
const TIMESERIES_TYPE_ENCODING_VERSION: i32 = 1;

pub static VK_TIME_SERIES_TYPE: ValkeyType = ValkeyType::new(
    "vktseries",
    TIMESERIES_TYPE_ENCODING_VERSION,
    RedisModuleTypeMethods {
        version: valkey_module::TYPE_METHOD_VERSION,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: None,
        free: Some(free),
        mem_usage: Some(mem_usage),
        digest: None,
        aux_load: None,
        aux_save: None,
        aux_save_triggers: REDISMODULE_AUX_BEFORE_RDB as i32,
        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
        aux_save2: None,
    },
);

fn remove_series_from_index(ts: &TimeSeries) {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    let saved_db = get_current_db(&guard);
    if saved_db != ts._db {
        set_current_db(&guard, ts._db);
    }
    with_timeseries_index(&guard, |ts_index| {
        ts_index.remove_timeseries(ts);
    });
    if saved_db != ts._db {
        set_current_db(&guard, saved_db);
    }
}

unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let series = &*value.cast::<TimeSeries>();
    rdb_save_series(series, rdb);
}

unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, enc_ver: c_int) -> *mut c_void {
    match rdb_load_series(rdb, enc_ver) {
        Ok(series) => Box::into_raw(Box::new(series)) as *mut std::ffi::c_void,
        Err(e) => {
            logging::log_notice(format!("Failed to load series from RDB. {e:?}"));
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let series = unsafe { &*(value as *mut TimeSeries) };
    series.memory_usage()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    let sm = value.cast::<TimeSeries>();
    // remove_series_from_index(&*sm);
    drop(Box::from_raw(sm));
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |index| {
        let old_series = &*value.cast::<TimeSeries>();
        let mut new_series = old_series.clone();
        new_series.id = next_timeseries_id();
        let key = ValkeyString::from_redis_module_string(guard.ctx, to_key);
        index.index_timeseries(&new_series, key.as_slice());
        let boxed = Box::new(new_series);
        Box::into_raw(boxed).cast::<c_void>()
    })
}

unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    let series = &*value.cast::<TimeSeries>();
    if value.is_null() {
        return;
    }
    remove_series_from_index(series);
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    if value.is_null() {
        return 0;
    }
    // Convert the pointer to a TimeSeries so we can operate on it.
    let series: &mut TimeSeries = &mut *(*value).cast::<TimeSeries>();
    match defrag_series(series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}
