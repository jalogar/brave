package brave.http;

import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpClientHandlerTest {
  List<Span> spans = new ArrayList<>();
  HttpTracing httpTracing;
  HttpSampler sampler = HttpSampler.TRACE_ID;
  @Mock HttpClientAdapter<Object, Object> adapter;
  @Mock TraceContext.Injector<Object> injector;
  Object request = new Object();
  HttpClientHandler<Object, Object> handler;

  @Before public void init() {
    httpTracing = HttpTracing.newBuilder(
        Tracing.newBuilder().spanReporter(spans::add).build()
    ).clientSampler(new HttpSampler() {
      @Override public <Req> Boolean trySample(HttpAdapter<Req, ?> adapter, Req request) {
        return sampler.trySample(adapter, request);
      }
    }).build();
    handler = HttpClientHandler.create(httpTracing, adapter);

    when(adapter.method(request)).thenReturn("GET");
  }

  @After public void close() {
    Tracing.current().close();
  }

  @Test public void handleSend_defaultsToMakeNewTrace() {
    assertThat(handler.handleSend(injector, request))
        .extracting(s -> s.isNoop(), s -> s.context().parentId())
        .containsExactly(false, null);
  }

  @Test public void handleSend_makesAChild() {
    brave.Span parent = httpTracing.tracing().tracer().newTrace();
    try (Tracer.SpanInScope ws = httpTracing.tracing().tracer().withSpanInScope(parent)) {
      assertThat(handler.handleSend(injector, request))
          .extracting(s -> s.isNoop(), s -> s.context().parentId())
          .containsExactly(false, parent.context().spanId());
    }
  }

  @Test public void handleSend_makesRequestBasedSamplingDecision() {
    sampler = mock(HttpSampler.class);
    // request sampler says false eventhough trace ID sampler would have said true
    when(sampler.trySample(adapter, request)).thenReturn(false);

    assertThat(handler.handleSend(injector, request).isNoop())
        .isTrue();
  }

  @Test public void handleSend_injectsTheTraceContext() {
    TraceContext context = handler.handleSend(injector, request).context();

    verify(injector).inject(context, request);
  }

  @Test public void handleSend_injectsTheTraceContext_onTheCarrier() {
    Object customCarrier = new Object();
    TraceContext context = handler.handleSend(injector, customCarrier, request).context();

    verify(injector).inject(context, customCarrier);
  }

  @Test public void handleSend_addsClientAddressWhenOnlyServiceName() {
    httpTracing = httpTracing.clientOf("remote-service");

    HttpClientHandler.create(httpTracing, adapter).handleSend(injector, request).finish();

    assertThat(spans)
        .extracting(Span::remoteServiceName)
        .containsExactly("remote-service");
  }

  @Test public void handleSend_skipsClientAddressWhenUnparsed() {
    handler.handleSend(injector, request).finish();

    assertThat(spans)
        .extracting(Span::remoteServiceName)
        .containsNull();
  }
}
