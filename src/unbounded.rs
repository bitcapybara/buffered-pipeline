use std::task::Poll;

use futures::{stream::FuturesUnordered, Future, Stream};
use pin_project::pin_project;

enum WaitState {
    // true for pending
    Futures,
    Stream,
    Sink,
}

#[pin_project]
pub struct BufferedPipeline<St, Si, Fut, Fo> {
    #[pin]
    stream: St,
    #[pin]
    futs: FuturesUnordered<Fut>,
    #[pin]
    sink: Si,
    buffer: std::collections::VecDeque<Fo>,
    state: WaitState,
}

impl<St, Si, Fut, Fo> BufferedPipeline<St, Si, Fut, Fo> {
    pub fn new(stream: St, sink: Si) -> Self {
        Self {
            stream,
            futs: FuturesUnordered::new(),
            sink,
            buffer: std::collections::VecDeque::new(),
            state: WaitState::Futures,
        }
    }
}

impl<St, Si, Fut, Se, Fo> Future for BufferedPipeline<St, Si, Fut, Fo>
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

        // priority: futuresUnordered > sink > stream

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
                WaitState::Stream => {
                    *this.state = loop {
                        match this.stream.as_mut().poll_next(cx) {
                            Poll::Ready(Some(item)) => {
                                this.futs.push(item);
                                println!(
                                    "new item from stream: futs: {}, buffer: {}, stream terminate: {}",
                                    this.futs.len(),
                                    this.buffer.len(),
                                    this.stream.is_terminated()
                                );
                            }
                            Poll::Ready(None) => {
                                // stream and buffer is empty, we need to wait for new future finishing
                                break WaitState::Futures;
                            }
                            Poll::Pending => {
                                if this.futs.is_empty() {
                                    return Poll::Pending;
                                }
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    };
                }
                WaitState::Sink => {
                    // sink buffer items
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
    use std::time::Instant;

    use futures::{FutureExt, StreamExt};
    use tokio_util::sync::PollSender;

    use super::*;

    #[tokio::test]
    async fn test_unbounded() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<usize>(5);

        tokio::spawn(async move {
            while let Some(res) = receiver.recv().await {
                println!("{res:?}")
            }
        });
        let start = Instant::now();
        BufferedPipeline::new(
            futures::stream::iter([
                async {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    5
                }
                .boxed(),
                async {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    4
                }
                .boxed(),
                async {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    3
                }
                .boxed(),
                async {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    2
                }
                .boxed(),
                async {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    1
                }
                .boxed(),
            ])
            .fuse(),
            PollSender::new(sender),
        )
        .await;
        print!("time: {:?}", start.elapsed());
    }
}
