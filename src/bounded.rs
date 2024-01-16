use std::task::Poll;

use futures::{stream::FuturesUnordered, Future, Stream};
use pin_project::pin_project;

enum WaitState {
    Futures,
    Stream,
    Sink,
}

#[pin_project]
pub struct BoundedPipeline<St, Si, Fut, Fo> {
    #[pin]
    stream: St,
    #[pin]
    futs: FuturesUnordered<Fut>,
    #[pin]
    sink: Si,
    buffer: std::collections::VecDeque<Fo>,
    capacity: usize,
    state: WaitState,
}

impl<St, Si, Fut, Fo> BoundedPipeline<St, Si, Fut, Fo> {
    pub fn new(stream: St, sink: Si, capacity: std::num::NonZeroUsize) -> Self {
        Self {
            stream,
            futs: FuturesUnordered::new(),
            sink,
            buffer: std::collections::VecDeque::new(),
            capacity: capacity.get(),
            state: WaitState::Futures,
        }
    }
}

impl<St, Si, Fut, Se, Fo> Future for BoundedPipeline<St, Si, Fut, Fo>
where
    Fut: Future<Output = Fo>,
    Si: futures::Sink<Fut::Output, Error = Se>,
    St: futures::stream::FusedStream<Item = Fut>,
{
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        *this.state = WaitState::Futures;

        loop {
            match this.state {
                WaitState::Futures => match this.futs.as_mut().poll_next(cx) {
                    Poll::Ready(Some(res)) => {
                        // get a finished future, continue
                        println!("new fut complete");
                        this.buffer.push_back(res);
                    }
                    Poll::Ready(None) => {
                        println!("futs empty!, buffer: {}", this.buffer.len());
                        if this.stream.is_terminated() {
                            *this.state = WaitState::Sink;
                            continue;
                        }
                        if !this.buffer.is_empty() {
                            // we need to empty the buffer first
                            *this.state = WaitState::Sink;
                        } else {
                            *this.state = WaitState::Stream;
                        }
                    }
                    Poll::Pending => {
                        if this.futs.len() + this.buffer.len() >= *this.capacity {
                            // the capacity is full, we can not receive more item from stream
                            if this.buffer.is_empty() {
                                // waiting for at least one future finishing
                                return Poll::Pending;
                            } else {
                                *this.state = WaitState::Sink;
                            }
                            continue;
                        }
                        if !this.buffer.is_empty() {
                            *this.state = WaitState::Sink;
                            continue;
                        }
                        // stream and buffer are all empty, we must wait for at least one future finishing
                        if this.stream.is_terminated() {
                            return Poll::Pending;
                        }
                        // now we can receive item from stream
                        *this.state = WaitState::Stream;
                    }
                },
                // poll item from stream as much as possible
                WaitState::Stream => {
                    // we need to poll futures immediately after adding new
                    let mut added_new = false;
                    loop {
                        match this.stream.as_mut().poll_next(cx) {
                            Poll::Ready(Some(item)) => {
                                this.futs.push(item);
                                added_new = true;
                                println!(
                                    "new item from stream: futs: {}, buffer: {}, stream terminate: {}",
                                    this.futs.len(),
                                    this.buffer.len(),
                                    this.stream.is_terminated()
                                );
                                if this.futs.len() + this.buffer.len() >= *this.capacity {
                                    break;
                                }
                            }
                            Poll::Ready(None) => {
                                if added_new {
                                    break;
                                }
                                // stream and buffer is empty, we need to wait for new future finishing
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            Poll::Pending => {
                                if added_new {
                                    break;
                                }
                                if this.futs.is_empty() {
                                    return Poll::Pending;
                                }
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }

                    *this.state = WaitState::Futures;
                }
                // sink buffer items as much as possible
                WaitState::Sink => {
                    loop {
                        match this.sink.as_mut().poll_ready(cx) {
                            Poll::Ready(_) => {}
                            Poll::Pending => {
                                println!("sink poll ready pending");
                                if this.futs.is_empty() {
                                    return Poll::Pending;
                                }
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                        let Some(res) = this.buffer.pop_front() else {
                            break;
                        };
                        match this.sink.as_mut().start_send(res) {
                            Ok(_) => {
                                // emit event
                                println!("send item success");
                            }
                            Err(_) => {
                                // emit event
                                println!("send item error")
                            }
                        }
                    }
                    match this.sink.as_mut().poll_flush(cx) {
                        Poll::Ready(_) => {
                            println!(
                                "futs: {}, buffer: {}, stream terminate: {}",
                                this.futs.len(),
                                this.buffer.len(),
                                this.stream.is_terminated()
                            );
                            if this.futs.is_empty()
                                && this.buffer.is_empty()
                                && this.stream.is_terminated()
                            {
                                println!("close!");
                                match this.sink.as_mut().poll_close(cx) {
                                    Poll::Ready(_) => return Poll::Ready(()),
                                    Poll::Pending => return Poll::Pending,
                                }
                            }
                        }
                        Poll::Pending => {
                            println!("sink flush pending!");
                            if this.futs.is_empty() {
                                return Poll::Pending;
                            }
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                    *this.state = WaitState::Futures;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Instant};

    use futures::StreamExt;
    use tokio_util::sync::PollSender;

    use super::*;

    #[tokio::test]
    async fn test_bounded() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(5);

        tokio::spawn(async move {
            while let Some(res) = receiver.recv().await {
                println!("{res:?}")
            }
        });
        let start = Instant::now();
        BoundedPipeline::new(
            futures::stream::iter([fut(5), fut(4), fut(3), fut(2), fut(1)]).fuse(),
            PollSender::new(sender),
            NonZeroUsize::new(2).unwrap(),
        )
        .await;
        print!("time: {:?}", start.elapsed());
    }

    async fn fut(num: u64) -> u64 {
        tokio::time::sleep(std::time::Duration::from_secs(num)).await;
        num
    }
}
