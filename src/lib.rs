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

pub struct StreamTaskMap<K, S, V> {
    tasks: HashMap<K, StreamTaskEntry>,

    sender: tokio::sync::mpsc::Sender<(K, Option<V>)>,
    general_output: tokio::sync::mpsc::Receiver<(K, Option<V>)>,

    _phantom: PhantomData<S>,
}

impl<K, S, V> Default for StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S, V> StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Clone + Send + 'static,
{
    #[must_use]
    pub fn new() -> Self {
        let (sender, general_output) = tokio::sync::mpsc::channel(0);

        Self {
            tasks: Default::default(),
            sender,
            general_output,
            _phantom: Default::default(),
        }
    }

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
                            let general_dropped = sender.send((key.clone(), v)).await.is_err();
                            is_none || general_dropped
                        };

                        let finished = tokio::select! {
                            _ = &mut drop_handle => {
                                true
                            }
                            done = fut => {
                                done
                            }
                        };

                        if finished {
                            return;
                        }
                    }
                }
            });

            StreamTaskEntry { task, dropper }
        });
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(state) = self.tasks.remove(key) {
            let _ = state.dropper.send(());
        }
    }
}

impl<K, S, V> Stream for StreamTaskMap<K, S, V>
where
    K: Clone + Eq + Hash + Send + Sync + Unpin + 'static,
    S: Stream<Item = V> + Send + Unpin + 'static,
    V: Clone + Send + 'static,
{
    type Item = (K, Option<V>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.general_output).poll_next(cx).map(|res| {
            if let Some((k, v)) = &res {
                if v.is_none() {
                    this.remove(k);
                }
            }

            res
        })
    }
}
