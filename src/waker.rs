use crate::Message;
use crossbeam_channel::Sender;
use futures::task::RawWaker;
use futures::task::RawWakerVTable;
use futures::task::Waker;
use slotmap::DefaultKey as Key;

pub(crate) fn new_waker(send_to: Sender<Message>, key: Key) -> Waker {
    WakerData::new(send_to, key).into_waker()
}

pub struct WakerData {
    send_to: Sender<Message>,
    key: Key,
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &WAKER_V_TABLE)
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

    pub(crate) fn into_waker(self) -> Waker {
        let leaked: *const Self = Box::leak(Box::new(self)) as *const _;
        let leaked: *const () = leaked as *const _;
        let raw_waker = RawWaker::new(leaked, &WAKER_V_TABLE);
        unsafe { Waker::new_unchecked(raw_waker) }
    }
}
