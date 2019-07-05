use std::env;
use std::sync::{Once, ONCE_INIT};

static ONCE: Once = ONCE_INIT;
static mut DEBUG: bool = false;

#[inline(always)]
pub(crate) fn is_debug_mode() -> bool {
    unsafe {
        ONCE.call_once(|| {
            DEBUG = match env::var("DEBUG_POOL") {
                Ok(val) => (&val == "1"),
                Err(_) => false,
            };
        });

        DEBUG
    }
}
