use crate::Message;
use crossbeam_channel::Sender;
use futures::task::RawWaker;
use futures::task::RawWakerVTable;
use futures::task::Waker;
use slotmap::DefaultKey as Key;
use std::mem;

pub(crate) fn new_waker(send_to: Sender<Message>, key: Key) -> Waker {
    WakerData::new(send_to, key).into_waker()
}

#[derive(Clone)]
pub struct WakerData {
    send_to: Sender<Message>,
    key: Key,
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    let data: Box<WakerData> = Box::from_raw(data as *mut () as *mut WakerData);
    let data_clone = data.clone();
    // Don't drop the first box since it's still in use!
    mem::forget(data);
    data_clone.into_raw_waker()
}

unsafe fn waker_wake(data: *const ()) {
    let data: &'static WakerData = &*(data as *const WakerData);
    let _ = data.send_to.send(Message::WakeFuture(data.key));
}

unsafe fn waker_drop(data: *const ()) {
    let data: Box<WakerData> = Box::from_raw(data as *mut () as *mut WakerData);
    drop(data);
}

const WAKER_V_TABLE: RawWakerVTable = RawWakerVTable {
    clone: waker_clone,
    wake: waker_wake,
    drop: waker_drop,
};

impl WakerData {
    pub(crate) fn new(send_to: Sender<Message>, key: Key) -> Self {
        WakerData { send_to, key }
    }

    fn into_raw_waker(self) -> RawWaker {
        let leaked: *const Self = Box::leak(Box::new(self)) as *const _;
        let leaked: *const () = leaked as *const _;
        RawWaker::new(leaked, &WAKER_V_TABLE)
    }

    pub(crate) fn into_waker(self) -> Waker {
        unsafe { Waker::new_unchecked(self.into_raw_waker()) }
    }
}
