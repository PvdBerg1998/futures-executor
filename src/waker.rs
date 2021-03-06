use crate::Message;
use crossbeam_channel::Sender;
use futures::task::{RawWaker, RawWakerVTable, Waker};
use slotmap::DefaultKey as Key;
use std::{mem, sync::Arc};

pub(crate) fn new_waker(send_to: Sender<Message>, key: Key) -> Waker {
    WakerData::new(send_to, key).into_waker()
}

pub struct WakerData {
    send_to: Sender<Message>,
    key: Key
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    // Increase strong count
    let arc: Arc<WakerData> = Arc::from_raw(data as *mut () as *mut WakerData);
    let arc_clone = arc.clone();
    mem::forget(arc);
    mem::forget(arc_clone);
    RawWaker::new(data, &WAKER_V_TABLE)
}

// Must take ownership of the data (drop it afterwards)
unsafe fn waker_wake(data: *const ()) {
    waker_wake_by_ref(data);
    waker_drop(data);
}

// Must not take ownership of the data
unsafe fn waker_wake_by_ref(data: *const ()) {
    let data: &WakerData = &*(data as *const WakerData);
    let _ = data.send_to.send(Message::WakeFuture(data.key));
}

unsafe fn waker_drop(data: *const ()) {
    // Decrease strong count
    let data: Arc<WakerData> = Arc::from_raw(data as *mut () as *mut WakerData);
    drop(data);
}

const WAKER_V_TABLE: RawWakerVTable =
    RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

impl WakerData {
    pub(crate) fn new(send_to: Sender<Message>, key: Key) -> Self {
        WakerData { send_to, key }
    }

    pub(crate) fn into_waker(self) -> Waker {
        let leaked: *const Self = Arc::into_raw(Arc::new(self)) as *const _;
        let leaked: *const () = leaked as *const _;
        let raw_waker = RawWaker::new(leaked, &WAKER_V_TABLE);
        unsafe { Waker::from_raw(raw_waker) }
    }
}
