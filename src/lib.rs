#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::default_trait_access)]

use std::{
    cmp::Eq,
    collections::HashMap,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{stream::*, task::JoinHandle};

#[allow(unused)]
struct StreamTaskEntry {
    task: JoinHandle<()>,
    dropper: tokio::sync::oneshot::Sender<()>,
}

pub enum StreamYield<S>
where
    S: Stream,
{
    Item(S::Item),
    Finished(S),
}

pub struct StreamTaskMap<K, S, V>
where
    S: Stream,
{
    tasks: HashMap<K, StreamTaskEntry>,

    sender: tokio::sync::mpsc::Sender<(K, StreamYield<S>)>,
    general_output: tokio::sync::mpsc::Receiver<(K, StreamYield<S>)>,

    _marker: PhantomData<V>,
}

impl<K, S, V> Default for StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S, V> StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Send + 'static,
{
    /// Create empty `StreamTaskMap`.
    #[must_use]
    pub fn new() -> Self {
        let (sender, general_output) = tokio::sync::mpsc::channel(0);

        Self {
            tasks: Default::default(),
            sender,
            general_output,
            _marker: Default::default(),
        }
    }

    /// Spawn stream in a separate task and add it to this `StreamTaskMap`.
    pub fn insert(&mut self, key: K, mut stream: S) {
        let Self { sender, tasks, .. } = self;
        tasks.entry(key.clone()).or_insert_with(|| {
            let (dropper, mut drop_handle) = tokio::sync::oneshot::channel();

            let task = tokio::spawn({
                let mut sender = sender.clone();
                async move {
                    loop {
                        // Await data from the stream and send it into general output when it has capacity.
                        // If the general output is dropped then discard the item.
                        let fut = async {
                            let v = stream.next().await;
                            let is_none = v.is_none();
                            let mut general_dropped = false;
                            if let Some(v) = v {
                                general_dropped = sender
                                    .send((key.clone(), StreamYield::Item(v)))
                                    .await
                                    .is_err();
                            }
                            is_none || general_dropped
                        };

                        // NOTE: if we await for general output while drop_handle is activated
                        // then the in-flight item may be lost.
                        let finished = tokio::select! {
                            _ = &mut drop_handle => {
                                true
                            }
                            done = fut => {
                                done
                            }
                        };

                        if finished {
                            let _ = sender.send((key.clone(), StreamYield::Finished(stream)));
                            return;
                        }
                    }
                }
            });

            StreamTaskEntry { task, dropper }
        });
    }

    /// Request stream termination. It will be returned as `StreamYield::Done`.
    pub fn terminate(&mut self, key: &K) {
        self.tasks.remove(key);
    }
}

impl<K, S, V> Stream for StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Send + Unpin + 'static,
{
    type Item = (K, StreamYield<S>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.general_output).poll_next(cx).map(|res| {
            if let Some((k, v)) = &res {
                if let StreamYield::Finished(_) = &v {
                    this.terminate(k);
                }
            }

            res
        })
    }
}
