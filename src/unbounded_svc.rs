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
pub struct UnboundedServicePipeline<St, Si, Svc, Fut, Fo> {
    #[pin]
    stream: St,
    #[pin]
    futs: FuturesUnordered<Fut>,
    #[pin]
    sink: Si,
    service: Svc,
    buffer: std::collections::VecDeque<Fo>,
    state: WaitState,
}

impl<St, Si, Svc, Fut, Fo> UnboundedServicePipeline<St, Si, Svc, Fut, Fo> {
    pub fn new(stream: St, service: Svc, sink: Si) -> Self {
        Self {
            stream,
            futs: FuturesUnordered::new(),
            sink,
            service,
            buffer: std::collections::VecDeque::new(),
            state: WaitState::Futures,
        }
    }
}

impl<St, Si, Svc, Fut, Fo> Future for UnboundedServicePipeline<St, Si, Svc, Fut, Fo>
where
    Svc: tower::Service<St::Item, Future = Fut>,
    Fut: Future<Output = Fo>,
    Si: futures::Sink<Fo>,
    St: futures::stream::FusedStream,
{
    type Output = Result<(), Svc::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        // priority: futuresUnordered > sink > service > stream
        *this.state = WaitState::Futures;

        loop {
            match this.state {
                // poll futures as much as possible
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
                // poll item from stream as much as possible
                WaitState::Stream => loop {
                    // check if service is ready
                    match this.service.poll_ready(cx) {
                        // service is ready, then poll a new item from stream
                        // TODO use StreamExt::ready_chunks
                        Poll::Ready(Ok(_)) => match this.stream.as_mut().poll_next(cx) {
                            Poll::Ready(Some(req)) => {
                                let fut = this.service.call(req);
                                this.futs.push(fut);
                                println!(
                                    "new item from stream: futs: {}, buffer: {}, stream terminate: {}",
                                    this.futs.len(),
                                    this.buffer.len(),
                                    this.stream.is_terminated()
                                );
                            }
                            Poll::Ready(None) => {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            Poll::Pending => {
                                if this.futs.is_empty() {
                                    return Poll::Pending;
                                }
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        },
                        // service is no more usable
                        Poll::Ready(Err(e)) => {
                            // TODO emit
                            return Poll::Ready(Err(e));
                        }
                        // service is not ready
                        Poll::Pending => {
                            if this.futs.is_empty() {
                                return Poll::Pending;
                            }
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                },
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
                                    Poll::Ready(_) => return Poll::Ready(Ok(())),
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
